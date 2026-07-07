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
  (:import [java.time Instant]
           [java.util UUID]))

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
             (tx* db [{:kaisha-proposal/id message-id
                       :kaisha-proposal/edn (enc content)}])
             {:proposal-id (str "denrei-pod/" message-id)})
           (post! [_ message-id content]
             (let [rec (channelport/delivery-record message-id content)]
               ;; durable fan-out fact FIRST — if the pod transact throws,
               ;; nothing claims the post happened (mirrors denrei.operation's
               ;; effect-before-record ordering one level down).
               (tx* db [{:kaisha-msg/id message-id
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
     :url   pod base URL (e.g. \"http://asher:8080\" on the tailnet)
     :graph target named graph (default: the actor identity's own
            key-derived IPNS name)
     :db-name tenant database name (live-edge tenant writes; see
            langchain.kotoba-db/kotoba-conn)
     :json-write :json-read injected JSON fns (e.g. data.json)
     :grant {:cap ... :scope ...} (default transact on :graph)
     :http-fn optional override (defaults to denrei.kotoba/jvm-http-fn)
     :deliverer optional post-transact hook"
  [{:keys [url graph db-name json-write json-read token cacao did identity
           grant http-fn deliverer]}]
  (let [graph (or graph (:graph identity))
        [cacao did]
        (if identity
          (let [now (str (Instant/now))
                g   (or grant {:cap :cap/transact :scope graph})]
            [(cacao/mint identity g {:aud url :nonce (str (UUID/randomUUID))
                                     :issued-at now
                                     :expiry (str (.plusSeconds (Instant/now) 3600))})
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
