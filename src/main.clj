(ns main
  (:gen-class)
  (:use util)
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :refer [lower-case join]]
   [ring.adapter.jetty :refer [run-jetty]]
   [ring.middleware.reload :refer [wrap-reload]]
   [hiccup.middleware :refer [wrap-base-url]]
   [hiccup.page :as page]
   [hiccup.form :as form]
   [hiccup2.core :as hiccup2]
   [hiccup.core :as hiccup]
   [bidi.ring :refer [make-handler]]
   [ring.util.response :refer [redirect]]
   [ring.util.codec :refer [url-encode]]
   [ring.middleware.params :refer [wrap-params]]
   [ring.middleware.multipart-params :refer [wrap-multipart-params]]
   [clojure.core.async :refer [chan <!! >!! <! go-loop close!]]
   [clojure.java.jdbc :as jdbc]
   [honey.sql :as sql]))

(def ID_SIZE 6)
(def MAX_SIZE 1024)

(defn gen-id! []
  (->> ID_SIZE
       range
       (map (fn [_] (rand-nth "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")))
       join))

(def db-spec {:dbtype "postgresql"
              :dbname "test"
              :host "localhost"
              :user "root"
              :password "root"
              :ssl false})

(defn db-do! [cmd]
  (jdbc/db-do-commands db-spec (sql/format cmd)))


(defn db-create-table! [table-name & columns]
  (db-do! {:drop-table [[:if-exists table-name]]})
  (db-do! {:create-table [table-name :if-not-exists]
           :with-columns columns}))

(defn db-reset! []
  (db-create-table!
   :files
   [:id [:char ID_SIZE] :primary-key]
   [:path [:varchar 255] :unique]))

(defn db-insert-file! [& paths]
  (db-do! {:insert-into :files
           :values (map (fn [path] {:id (gen-id!), :path path}) paths)}))

(defonce server (atom nil))

(defn server-restart! []
  (doto @server
    (.stop)
    (.start)))

(defn sha-256 ^String [^String s]
  (->> s
       (map byte)
       byte-array
       (.digest (java.security.MessageDigest/getInstance "SHA-256"))
       (map (partial format "%02x"))
       join))

(def event-queue (chan 10))

(defn event-hook-out! [event]
  (let [out (chan)]
    (>!! event-queue (assoc event :out out))
    (<!! out)))

(def routes
  ["/"
   {:get
    {"" (fn [_]
          {:status 200
           :body [:div {:style "display: grid; justify-content: center;"}
                  [:h1 "upload a file"]
                  [:form {:method "POST"
                          :action "/"
                          :enctype "multipart/form-data"}
                   (form/label "file" "file:")
                   (form/file-upload "file")
                   (form/submit-button "send!")]]})}

    :post
    {"" (fn [request]
          (let [params (get-in request [:params "file"])
                name (:filename params)
                size (:size params)]

            (if (>= size MAX_SIZE)
              {:status 200, :body "nunda"}
              (let [contents (-> params :tempfile slurp)]
                (println "---")
                (pprint params)
                (println (sha-256 contents))))))}}])

(def app
  (-> routes
      make-handler
      wrap-base-url
      wrap-multipart-params
      (pipe
       (fn [handler request]
         (-> (handler request)
             (update :body
                     #(str
                       (page/html5
                        (page/include-css "https://cdn.jsdelivr.net/npm/holiday.css@0.11.6")
                        %)))
             (update :headers (partial merge {"Content-Type" "text/html"})))))))

;; (def consumer
;;   (go-loop []
;;     (let [event (<! event-queue)]
;;       (condp = (:type event)
;;         :file
;;         (let [{:keys []}])))

;;     (recur)))

(defn -main []
  (reset! server (run-jetty (wrap-reload #'app) {:port 8080 :join? false})))
