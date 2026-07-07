(ns denrei.phase
  "Phase 0→3 staged rollout, gating only the ASSESS ops (draft/post
  decisions). Recording ground facts (a space appearing via :space/register)
  is always on — that is denrei's observe charter (durable ground facts). The
  phase only decides how much autonomy *drafting* has; posting is never in
  scope for autonomy — it is a separate, always-human charter enforced by the
  governor's high-stakes flag, not by phase.

    0 ingest-only    — record spaces (channels/members as ground facts); emit
                       NO drafts yet (shadow space).
    1 assisted       — draft allowed, but always human even to commit just the
                       draft content.
    2 assisted-draft — a clean+confident draft may auto-commit (it is just
                       proposed content sitting on the message for review);
                       posting stays human.
    3 supervised     — same autonomy as 2; posting is high-stakes and ALWAYS
                       routes to a human (regardless of phase).")

(def record-ops #{:space/register})
(def assess-ops #{:message/draft :message/post})

(def phases
  {0 {:label "ingest-only"    :assess #{}        :auto #{}}
   1 {:label "assisted"       :assess assess-ops :auto #{}}
   2 {:label "assisted-draft" :assess assess-ops :auto #{:message/draft}}
   3 {:label "supervised"     :assess assess-ops :auto #{:message/draft}}})

(def default-phase 3)

(defn record-op? [op] (contains? record-ops op))

(defn gate
  "Adjust an assess op's governor disposition for the rollout phase.
  Returns {:disposition kw :reason kw|nil}. `:message/post` is never in
  :auto, so it always escalates; the governor's high-stakes flag already
  forces this too — phase and governor agree by construction."
  [phase {:keys [op]} disposition]
  (let [{:keys [assess auto]} (get phases phase (get phases default-phase))]
    (cond
      (= :hold disposition)        {:disposition :hold :reason nil}
      (not (contains? assess op))  {:disposition :hold :reason :phase-disabled}
      (and (= :commit disposition)
           (not (contains? auto op))) {:disposition :escalate :reason :phase-approval}
      :else                        {:disposition disposition :reason nil})))

(defn verdict->disposition [v]
  (cond (:hard? v) :hold (:escalate? v) :escalate :else :commit))
