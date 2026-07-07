(ns denrei.store-contract-test
  "Store contract against both backends — proving MemStore ≡ DatomicStore makes
  'swap the SSoT for Datomic / kotoba-server' a config change, not a rewrite."
  (:require [clojure.test :refer [deftest is testing]]
            [kaisha.model :as k]
            [denrei.store :as store]))

(defn- backends [] [["MemStore" (store/seed-db)] ["DatomicStore" (store/datomic-seed-db)]])

(deftest read-parity
  (doseq [[label s] (backends)]
    (testing label
      (is (= "cloud-itonami" (:repo (store/activity s "act-standup"))))
      (is (= :known (:consent (store/contact s "alice"))))
      (is (= :blocked (:consent (store/contact s "blocked"))))
      (is (true? (:first-contact? (store/contact s "newbiz"))))
      (is (= "cloud-itonami" (:tenant (store/space s "gftd"))))
      (is (some? (k/channel-by-id (store/space s "gftd") "general")))
      (is (contains? (:kaisha/members (k/channel-by-id (store/space s "gftd") "general")) "denrei"))
      (is (not (contains? (:kaisha/members (k/channel-by-id (store/space s "gftd") "ops")) "denrei")))
      (is (= 1 (count (store/all-spaces s))))
      (is (nil? (store/space s "sp-missing")))
      (is (nil? (store/activity s "act-missing"))))))

(deftest write-and-ledger-parity
  (doseq [[label s] (backends)]
    (testing label
      (store/record-datom! s {:kind :draft :id "msg-1" :value {:content {:channel "general"} :status :proposed}})
      (is (= :proposed (:status (store/draft-of s "msg-1"))))
      (store/record-datom! s {:kind :draft :id "msg-1" :value {:status :posted}})
      (is (= :posted (:status (store/draft-of s "msg-1"))) "merge updates status")
      (is (some? (:content (store/draft-of s "msg-1"))) "merge preserves other fields")
      (store/record-datom! s {:kind :space :id "gftd" :value {:kaisha/name "GFTD (revised)"}})
      (is (= "GFTD (revised)" (:kaisha/name (store/space s "gftd"))))
      (store/append-ledger! s {:op :a :disposition :record})
      (store/append-ledger! s {:op :b :disposition :commit})
      (is (= [:record :commit] (mapv :disposition (store/ledger s)))))))

(deftest datomic-empty-store-usable
  (let [s (store/datomic-store)]
    (is (nil? (store/space s "nope")))
    (is (= [] (store/all-spaces s)))
    (store/record-datom! s {:kind :space :id "x"
                            :value (merge (k/space "x") {:tenant "t"})})
    (is (= "t" (:tenant (store/space s "x"))))))
