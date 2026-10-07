(ns util)

(defn pipe [init & fs]
  (reduce (fn [acc f] (partial f acc)) init fs))
