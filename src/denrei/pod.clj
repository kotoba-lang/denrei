(ns denrei.pod
  "Live ChannelTarget backed by a kaisha pod — a kotoba-server datom graph
  (ADR-2607072400: the realtime pod lives on the murakumo fleet's resident
  kotoba-server; reachability is tailnet today, murakumo.cloud overlay later;
  kotobase.net keeps the public HTTP face).

  The target speaks ONLY the langchain.db `:db-api` map ({:q :transact! :db
  :pull :entid}) — `langchain.db/api` (in-process EAVT) and
  `langchain.kotoba-db/kotoba-api` (kotoba-server XRPC) both implement it, so
  the SAME ChannelTarget runs against an in-memory store (tests, offline) or
  a live fleet pod (production) by construction — the exact swap
  denrei.store's MemStore ≡ DatomicStore contract already proves for the SSoT.

  Delivery model: `post!` transacts the delivery record (the governed wire
  form denrei.channelport/delivery-record builds — mention set included) into
  the pod graph as durable datoms, indexed by channel
  (`:kaisha-msg/channel` = \"<space>/<channel>\"). Fan-out consumers (a
  murakumo lattice `on-kse` WASM component, a kaisha UI, another actor) read
  that index — `channel-messages` below is the reference query. The pod graph
  IS the fan-out substrate; no side-channel webhook to keep consistent with
  what was governed.

  post! is only ever called by denrei.operation's commit step, after human
  approval — same charter as every other ChannelTarget."
  (:require [clojure.edn :as edn]
            [denrei.cacao :as cacao]
            [denrei.channelport :as channelport]
            [denrei.kotoba :as kotoba]
            [langchain.db :as d]
            [langchain.kotoba-db :as kdb])
  (:import [java.security MessageDigest]
           [java.time Instant]
           [java.time.temporal ChronoUnit]
           [java.util UUID]))

;; ───────── fleet-node graph addressing (kotoba-server, kotoba-core/cid) ─────────

(def ^:private b32-alphabet "abcdefghijklmnopqrstuvwxyz234567")

(defn- base32-lower-no-pad [^bytes bs]
  (let [bits (mapcat (fn [b] (map #(bit-and 1 (bit-shift-right (bit-and b 0xff) %))
                                  (range 7 -1 -1)))
                     bs)]
    (apply str (map (fn [chunk] (nth b32-alphabet (reduce (fn [a x] (+ (* 2 a) x)) 0 chunk)))
                    (partition 5 5 (repeat 0) bits)))))

(defn graph-cid-from-name
  "kotoba-core KotobaCid/from_bytes over a graph name: CIDv1/dag-cbor/sha2-256
  header (0x01 0x71 0x12 0x20) + sha256(name), multibase base32-lower 'b'."
  [^String nm]
  (let [h (.digest (MessageDigest/getInstance "SHA-256") (.getBytes nm "UTF-8"))]
    (str "b" (base32-lower-no-pad
              (byte-array (concat [0x01 0x71 0x12 0x20] (seq h)))))))

(defn private-graph-cid
  "The actor's account-owned private graph on a fleet kotoba-server:
  NamedGraph::private_for(did) = CID of \"kotoba://graph/private/<did>\".
  A datomic.transact scoped to exactly this CID auto-registers the graph
  Private{owner = CACAO issuer} on first write (kotoba-server xrpc.rs, data-
  sovereignty path) — the CID binds to the DID, so only the matching issuer
  can ever claim it. Verified live against a murakumo fleet node 2026-07-07."
  [did]
  (graph-cid-from-name (str "kotoba://graph/private/" did)))

(def schema
  "Datom schema for the kaisha pod graph. Also usable to seed an in-process
  langchain.db conn (tests / offline dev)."
  {:kaisha-msg/id      {:db/unique :db.unique/identity}
   :kaisha-proposal/id {:db/unique :db.unique/identity}})

(defn- enc [v] (pr-str v))
(defn- dec* [s] (when s (edn/read-string s)))

(defn- q* [{:keys [api conn]} query & inputs]
  (apply (:q api) query ((:db api) conn) inputs))
(defn- pull* [{:keys [api conn]} pattern eid] ((:pull api) ((:db api) conn) pattern eid))
(defn- tx* [{:keys [api conn]} txd] ((:transact! api) conn txd))

(defn- channel-key [space channel] (str space "/" channel))

(defn channel-messages
  "Reference fan-out read: every delivery record posted to <space>/<channel>,
  oldest first (by :at, then id) — the query an on-kse component / kaisha UI
  runs against the pod graph. `target` is the map returned by db-channelport."
  [{:keys [db] :as _target} space channel]
  (->> (q* db '[:find [?edn ...]
                :in $ ?c
                :where [?e :kaisha-msg/channel ?c]
                       [?e :kaisha-msg/edn ?edn]]
           (channel-key space channel))
       (map dec*)
       (sort-by (juxt :at :message-id))
       vec))

(defn db-channelport
  "A denrei.channelport/ChannelTarget over any langchain.db `:db-api` conn.
  Returns {:target <ChannelTarget> :db {:api :conn}} — keep the map around
  for `channel-messages`. Optional `deliverer` is invoked with each posted
  delivery record AFTER the transact succeeds (e.g. a realtime nudge); the
  durable fan-out substrate is the graph itself, so the default is a no-op."
  ([api conn] (db-channelport api conn (fn [_] nil)))
  ([api conn deliverer]
   (let [db {:api api :conn conn}
         target
         (reify channelport/ChannelTarget
           (fetch-message [_ message-id]
             (-> (pull* db [:kaisha-msg/edn] [:kaisha-msg/id message-id])
                 :kaisha-msg/edn dec*))
           (propose-revision! [_ message-id content]
             ;; explicit :db/id — a fleet kotoba-server's transact dialect
             ;; requires it ("entity map must contain :db/id", observed live
             ;; 2026-07-07); langchain.db accepts it identically.
             (tx* db [{:db/id (str "kaisha-proposal/" message-id)
                       :kaisha-proposal/id message-id
                       :kaisha-proposal/edn (enc content)}])
             {:proposal-id (str "denrei-pod/" message-id)})
           (post! [_ message-id content]
             (let [rec (channelport/delivery-record message-id content)]
               ;; durable fan-out fact FIRST — if the pod transact throws,
               ;; nothing claims the post happened (mirrors denrei.operation's
               ;; effect-before-record ordering one level down).
               (tx* db [{:db/id (str "kaisha-msg/" message-id)
                         :kaisha-msg/id message-id
                         :kaisha-msg/channel (channel-key (:space rec) (:channel rec))
                         :kaisha-msg/edn (enc rec)}])
               (deliverer rec)
               rec)))]
     {:target target :db db})))

(defn in-process-channelport
  "db-channelport on the in-process langchain.db EAVT backend — the offline/
  test variant, byte-compatible with the pod one by the `:db-api` contract."
  ([] (in-process-channelport (fn [_] nil)))
  ([deliverer] (db-channelport d/api (d/create-conn schema) deliverer)))

(defn kotoba-channelport
  "db-channelport against a live kaisha pod (kotoba-server over XRPC — a
  murakumo fleet node reached over the tailnet today, per ADR-2607072400).

   Auth options (pick one, same shape as denrei.kotoba/kotoba-store):
     :token          Bearer JWT (handed)
     :cacao + :did   a ready CACAO b64 + signer DID
     :identity       a denrei.cacao identity — the actor SELF-MINTS a
                     :cap/transact CACAO for its own key-derived graph
                     (posting writes datoms, so the default grant is
                     transact, not read).
   opts:
     :url   pod base URL (e.g. \"http://asher:8077\" on the tailnet)
     :aud   CACAO audience for the self-mint (default :url). A murakumo
            fleet kotoba-server verifies the audience against its own node
            DID (did:key of the node identity) — pass that DID here; the
            node discloses it in the 401 body on mismatch.
     :graph target named graph (default: the actor identity's own
            key-derived IPNS name; a fleet node requires a canonical graph
            CID — see e.g. syosetsuka.cacao/canonical-graph)
     :db-name tenant database name (live-edge tenant writes; see
            langchain.kotoba-db/kotoba-conn)
     :json-write :json-read injected JSON fns (e.g. data.json)
     :grant {:cap ... :scope ...} (default transact on :graph)
     :http-fn optional override (defaults to denrei.kotoba/jvm-http-fn)
     :deliverer optional post-transact hook"
  [{:keys [url aud graph db-name json-write json-read token cacao did identity
           grant http-fn deliverer]}]
  (let [graph (or graph (:graph identity))
        [cacao did]
        (if identity
          ;; fleet kotoba-server rejects fractional-second timestamps
          ;; ("must be YYYY-MM-DDTHH:MM:SSZ UTC") — truncate to seconds.
          ;; default grant covers the target's full surface: reads
          ;; (fetch-message/channel-messages → datom:read), writes
          ;; (datom:transact), and first-write graph creation (tx:create) —
          ;; a fleet node checks all of them (observed live 2026-07-07).
          (let [now-i (.truncatedTo (Instant/now) ChronoUnit/SECONDS)
                g     (or grant {:cap [:cap/read :cap/transact :cap/admin]
                                 :scope graph})]
            [(cacao/mint identity g {:aud (or aud url) :nonce (str (UUID/randomUUID))
                                     :issued-at (str now-i)
                                     :expiry (str (.plusSeconds now-i 3600))})
             (:did identity)])
          [cacao did])
        host-caps {:http-fn (or http-fn kotoba/jvm-http-fn)
                   :json-write json-write :json-read json-read}
        api  (kdb/kotoba-api host-caps)
        conn (kdb/kotoba-conn url graph (cond-> {}
                                          token   (assoc :token token)
                                          cacao   (assoc :cacao cacao :did did)
                                          db-name (assoc :db-name db-name)))]
    (db-channelport api conn (or deliverer (fn [_] nil)))))

(defn fleet-channelport
  "kotoba-channelport against a murakumo fleet node, with every fleet-node
  requirement pre-wired (all verified live against asher 2026-07-07,
  ADR-2607072400):
    - graph = the actor's account-owned private graph
      (`private-graph-cid` — auto-registers Private{owner=actor} on first
      write; the node rejects IPNS names / libp2p-key CIDs as graph params)
    - CACAO aud = the NODE's did:key (:node-did — the node discloses it in
      the 401 body on mismatch), not the URL
    - multi-cap grant [read transact tx:create] (the node checks
      datom:transact AND tx:create on a first write, datom:read on reads)
    - second-precision CACAO timestamps (fractional seconds rejected)

  opts: :url (e.g. \"http://100.x.y.z:8077\" on the tailnet — fleet.edn's
  :fleet/port default is 8077), :identity (denrei.cacao identity),
  :node-did, :json-write :json-read, optional :deliverer."
  [{:keys [identity node-did] :as opts}]
  (let [did  (:did identity)
        gcid (private-graph-cid did)]
    (kotoba-channelport
     (-> opts
         (dissoc :node-did)
         (assoc :aud node-did
                :graph gcid
                ;; the node's WRITE path scope-checks the graph CID, but its
                ;; READ path canonicalizes a registered graph to its name form
                ;; ("private/<did>") before the scope check (observed live
                ;; 2026-07-07) — grant both so one CACAO covers both.
                :grant {:cap [:cap/read :cap/transact :cap/admin]
                        :scope [gcid (str "private/" did)]})))))
