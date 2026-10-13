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
   [honey.sql :as sql]
   [honey.sql.helpers :as hsql]))

(def ID_SIZE 64) ;; sha-256
(def MAX_SIZE 1024)

(def db-spec {:dbtype "postgresql"
              :dbname "test"
              :host "localhost"
              :user "root"
              :password "root"
              :ssl false})

(defn db-do! [cmd]
  (jdbc/execute! db-spec (sql/format cmd)))


(defn db-create-table! [table-name & columns]
  (db-do! {:drop-table [[:if-exists table-name]]})
  (db-do! {:create-table [table-name :if-not-exists]
           :with-columns columns}))

(defn db-reset! []
  (db-create-table!
   :files
   [:id [:char ID_SIZE] :primary-key]
   [:name [:varchar 255] :unique]))

(defn db-insert-file! [id name]
  (db-do! {:insert-into :files
           :columns [:id :name]
           :values [[id name]]}))

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
                   (form/submit-button "send!")]]})
     ["c/" :link] (fn [{{:keys [link]} :route-params}]
                    {:status 200, :body [:a {:href (str "/f/" link)} link]})}

    ["f/" :link] (fn [{{:keys [link]} :route-params}]
                   (event-hook-out! {:type :get-file :file link}))

    :post
    {"" (fn [request]
          (let [params (get-in request [:params "file"])]
            (event-hook-out!
             {:type :file
              :name (:filename params)
              :size (:size params)
              :content (:tempfile params)})))}}])

(def app
  (-> routes
      make-handler
      wrap-base-url
      wrap-multipart-params
      (pipe
       (fn [handler request]
         (let [result (handler request)]
           (if (not= (-> result :body type) clojure.lang.PersistentVector)
             result
             (-> result
                 (update :body
                         #(str
                           (page/html5
                               (page/include-css "https://cdn.jsdelivr.net/npm/holiday.css@0.11.6")
                               %)))
                 (update :headers (partial merge {"Content-Type" "text/html"})))))))))

(def consumer
  (go-loop []
    (let [event (<! event-queue)]
      (pprint event)

      (condp = (:type event)
        :file
        (let [{:keys [name size content out]} event]
          (if (>= size MAX_SIZE)
            {:status 200
             :body "nunda"}
            (let [file (slurp content)
                  hash (sha-256 file)]
              (spit (str "./files/" hash) file)
              (db-insert-file! hash name)
              (>!! out (->> hash (str "/c/") redirect)))))

        :get-file
        (let [{:keys [file out]} event
              name (->>
                    {:select :name
                     :from :files
                     :where [:= :id file]}
                    sql/format
                    (jdbc/query db-spec)
                    first
                    :name)]
          (>!! out (if name
                     (do
                       {:status 200
                        :headers {"Content-Type" "application/octet-stream"
                                  "Content-Disposition" (str "attachment; filename=" name)}
                        :body (->> file (str "./files/") slurp)})
                     {:status 404, :body [:div
                                          [:h1 "not found"]
                                          [:pre file]]})))))
    (recur)))

(defn -main []
  (reset! server (run-jetty (wrap-reload #'app) {:port 8080 :join? false})))
