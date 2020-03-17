(ns raft-kv.core
  (:gen-class)
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [compojure.core :as compojure :refer [GET POST]]
            [ring.middleware.params :as params]
            [compojure.route :as route]
            [clojure.core.async :as async]
            [fluree.raft :as raft]
            [raft-kv.ftcp :as ftcp]
            [aleph.http :as http]
            [clojure.tools.logging :as log]
            [cheshire.core :as cjson]
            [aleph.netty :as netty]
            [byte-streams :as bs]
            [clojure.string :as str]
            [manifold.deferred :as d])
  (:refer-clojure :exclude [read])
  (:import (java.util UUID)
           (java.io File ByteArrayInputStream InputStream)))

(def system nil)

;; This is virtually identical to how we run RAFT for Fluree, except the state-machine is only
;; a key-val store
(defn state-machine
  "Basic key-val store.

  Operations are tuples that look like:
  [operation key val compare-val]

  Operations supported are:
  - :write  - Writes a new value to specified key. Returns true on success.
              i.e. [:write 'mykey' 42]
  - :read   - Reads value of provided key. Returns nil if value doesn't exist.
              i.e. [:read 'mykey']
  - :delete - Deletes value at specified key. Returns true if successful, or
              false if key doesn't exist. i.e. [:delete 'mykey']
  - :cas    - Compare and swap. Compare current value of key with v and if equal,
              swap val to cas-v. Returns true on success and false on failure.
              i.e. [:cas 'mykey' 100 42] - swaps 'mykey' to 42 if current val is 100."
  [state-atom]
  (fn [[op k v cas-v] raft-state]
    (case op
      "read" (get @state-atom k)
      "write" (do (swap! state-atom assoc k v)
                 true)
      "cas" (if (contains? @state-atom k)
             (let [new-state (swap! state-atom
                                    (fn [state]
                                      (if (= v (get state k))
                                        (assoc state k cas-v)
                                        state)))]
               (= cas-v (get new-state k)))
             false)
      "delete" (if (contains? @state-atom k)
                (do (swap! state-atom dissoc k)
                    true)
                false))))

(defn snapshot-xfer
  "Transfers snapshot from this server as leader, to a follower.
  Will be called with two arguments, snapshot id and part number.
  Initial call will be for part 1, and subsequent calls, if necessary,
  will be for each successive part.

  Must return a snapshot with the following fields
  :parts - how many parts total
  :data - snapshot data

  If multiple parts are returned, additional requests for each part will be
  requested. A snapshot should be broken into multiple parts if it is larger than
  the amount of data you want to push across the network at once."
  [path]
  (fn [id part]
    ;; in this example we do everything in one part, regardless of snapshot size
    (let [file (io/file path (str id ".snapshot"))
          ba   (byte-array (.length file))
          is   (io/input-stream file)]
      (.read is ba)
      (.close is)
      {:parts 1
       :data  ba})))


(defn snapshot-installer
  "Installs a new snapshot being sent from a different server.
  Blocking until write succeeds. An error will stop RAFT entirely.

  If snapshot-part = 1, should first delete any existing file if it exists (possible to have historic partial snapshot lingering).

  As soon as final part write succeeds, can safely garbage collect any old snapshots on disk except the most recent one."
  [path]
  (fn [snapshot-map]
    (let [{:keys [leader-id snapshot-term snapshot-index snapshot-part snapshot-parts snapshot-data]} snapshot-map
          file (io/file path (str snapshot-index ".snapshot"))]

      (when (= 1 snapshot-part)
        ;; delete any old file if exists
        (io/make-parents file)
        (io/delete-file file true))

      (with-open [out (io/output-stream file :append true)]
        (.write out ^bytes snapshot-data)))))


(defn snapshot-reify
  "Reifies a snapshot, should populate whatever data is needed into an initialized state machine
  that is used for raft.

  Called with snapshot-id to reify, which corresponds to the commit index the snapshot was taken.
  Should throw if snapshot not found, or unable to parse. This will stop raft."
  [path state-atom]
  (fn [snapshot-id]
    (try
      (let [file  (io/file path (str snapshot-id ".snapshot"))
            state (nippy/thaw-from-file file)]
        (reset! state-atom state))
      (catch Exception e (log/error e "Error reifying snapshot: " snapshot-id)))))


(defn- return-snapshot-id
  "Takes java file and returns log id (typically same as start index)
  from the file name as a long integer."
  [^File file]
  (when-let [match (re-find #"^([0-9]+)\.snapshot$" (.getName file))]
    (Long/parseLong (second match))))


(defn- purge-snapshots
  [path max-snapshots]
  (let [rm-snapshots (some->> (file-seq (clojure.java.io/file path))
                              (filter #(.isFile ^File %))
                              (keep return-snapshot-id)
                              (sort >)
                              (drop max-snapshots))]
    (when (not-empty rm-snapshots)
      (log/info "Removing snapshots: " rm-snapshots))
    (doseq [snapshot rm-snapshots]
      (let [file (io/file path (str snapshot ".snapshot"))]
        (io/delete-file file true)))))

(defn snapshot-writer
  "Blocking until write succeeds. An error will stop RAFT entirely."
  [path state-atom]
  (fn [index callback]
    (log/info "Ledger group snapshot write triggered for index: " index)
    (let [start-time    (System/currentTimeMillis)
          state         @state-atom
          file          (io/file path (str index ".snapshot"))
          max-snapshots 6]
      (io/make-parents file)
      (future
        (try (nippy/freeze-to-file file state)
             (catch Exception e (log/error e "Error writing snapshot index: " index)))
        (log/info (format "Ledger group snapshot completed for index %s in %s milliseconds."
                          index (- (System/currentTimeMillis) start-time)))
        (callback)
        (purge-snapshots path max-snapshots)))))


;; Holds state change functions that are registered
(def state-change-fn-atom (atom {}))

(defn register-state-change-fn
  "Registers function to be called with every state monitor change. id provided is used to un-register function
  and is otherwise opaque to the system."
  [id f]
  (swap! state-change-fn-atom assoc id f))

(defn unregister-state-change-fn
  [id]
  (swap! state-change-fn-atom dissoc id))


(defn unregister-all-state-change-fn
  []
  (reset! state-change-fn-atom {}))

;; map of request-ids to a response channel that will contain the response eventually
(def pending-responses (atom {}))


(defn send-rpc
  "Sends rpc call to specified server.
  Includes a resp-chan that will eventually contain a response.

  Returns true if successful, else will return an exception if
  connection doesn't exist (not established, or closed)."
  [raft server operation data callback]
  (let [this-server (:this-server raft)
        msg-id      (str (UUID/randomUUID))
        header      {:op     operation
                     :from   this-server
                     :to     server
                     :msg-id msg-id}]
    (when (fn? callback)
      (swap! pending-responses assoc msg-id callback))
    (let [success? (ftcp/send-rpc this-server server header data)]
      (if success?
        (log/trace "send-rpc success:" {:op operation :data data :header header})
        (do
          (swap! pending-responses dissoc msg-id)
          (log/debug "Connection to" server "is closed, unable to send rpc. " (pr-str header)))))))

(defn message-consume
  "Function used to consume inbound server messages.

  client-id should be an approved client-id from the initial
  client negotiation process, can be can used to validate incoming
  messages are labeled as coming from the correct client."
  [raft conn message]
  (try
    (let [message'  (nippy/thaw message)
          [header data] message'
          {:keys [op msg-id]} header
          response? (str/ends-with? (name op) "-response")
          {:keys [write-chan]} conn]
      (if response?
        (let [callback (get @pending-responses msg-id)]
          (when (fn? callback)
            (swap! pending-responses dissoc msg-id)
            (callback data)))
        (let [resp-header (assoc header :op (keyword (str (name op) "-response"))
                                        :to (:from header)
                                        :from (:to header))
              callback    (fn [x]
                            (ftcp/send-message write-chan resp-header x))]
          (case op
            ;:storage-read
            ;(let [file-key data]
            ;  (log/debug "Storage read for key: " file-key)
            ;  (async/go
            ;    (-> (storage/storage-read {:storage-read key-storage-read-fn} file-key)
            ;        (async/<!)
            ;        (callback))))

            :new-command
            (let [{:keys [id entry]} data
                  command (raft/map->RaftCommand {:entry entry
                                                  :id    id})]
              (log/debug "Raft - new command:" (pr-str data))
              (raft/new-command raft command callback))

            ;; else
            (raft/invoke-rpc raft op data callback)))))
    (catch Exception e (log/error e "Error consuming new message! Ignoring."))))


;; start with a default state when no other state is present (initialization)
;; useful for setting a base 'version' in state
(def default-state {:version 3})


(defn start-instance
  [raft-config]
  (let [{:keys [port this-server log-directory entries-max
                storage-read storage-write private-keys open-api]} raft-config
        event-chan             (async/chan)
        command-chan           (async/chan)
        state-machine-atom     (atom default-state)
        log-directory          (or log-directory (str "raftlog/" (name this-server) "/"))
        raft-config*           (assoc raft-config :event-chan event-chan
                                                  :command-chan command-chan
                                                  :send-rpc-fn send-rpc
                                                  :retain-logs 5
                                                  :log-directory log-directory
                                                  :entries-max (or entries-max 50)
                                                  :state-machine (state-machine state-machine-atom)
                                                  :snapshot-write (snapshot-writer (str log-directory "snapshots/") state-machine-atom)
                                                  :snapshot-reify (snapshot-reify (str log-directory "snapshots/") state-machine-atom)
                                                  :snapshot-xfer (snapshot-xfer (str log-directory "snapshots/"))
                                                  :snapshot-install (snapshot-installer (str log-directory "snapshots/")))
        _                      (log/debug "Starting Raft with config:" (pr-str raft-config*))
        raft                   (raft/start raft-config*)
        client-message-handler (partial message-consume raft)
        new-client-handler     (fn [client]
                                 (ftcp/monitor-remote-connection this-server client client-message-handler nil))
        ;; both starts server and returns a shutdown function
        server-shutdown-fn     (ftcp/start-tcp-server port new-client-handler)]

    {:raft            raft
     :state-atom      state-machine-atom
     :port            port
     :server-shutdown server-shutdown-fn
     :this-server     this-server
     :event-chan      event-chan
     :command-chan    command-chan
     :private-keys    private-keys
     :open-api        open-api}))



(defn view-raft-state
  "Returns current raft state to callback."
  ([raft] (view-raft-state raft (fn [x] (clojure.pprint/pprint (dissoc x :config)))))
  ([raft callback]
   (raft/view-raft-state (:raft raft) callback)))


(defn view-raft-state-async
  "Returns current raft state as a core async channel."
  [raft]
  (let [resp-chan (async/promise-chan)]
    (raft/view-raft-state (:raft raft)
                          (fn [rs]
                            (async/put! resp-chan rs)
                            (async/close! resp-chan)))
    resp-chan))


(defn monitor-raft
  "Monitor raft events and state for debugging"
  ([raft] (monitor-raft raft (fn [x] (clojure.pprint/pprint x))))
  ([raft callback]
   (raft/monitor-raft (:raft raft) callback)))


(defn monitor-raft-stop
  "Stops current raft monitor"
  [raft]
  (raft/monitor-raft (:raft raft) nil))


(defn leader-async
  "Returns leader as a core async channel once available.
  Default timeout supplied, or specify one."
  ([raft] (leader-async raft 60000))
  ([raft timeout]
   (let [timeout-time (+ (System/currentTimeMillis) timeout)]
     (async/go-loop [retries 0]
       (let [resp-chan (async/promise-chan)
             _         (view-raft-state raft (fn [state]
                                               (if-let [leader (:leader state)]
                                                 (async/put! resp-chan leader)
                                                 (async/close! resp-chan))))
             resp      (async/<! resp-chan)]
         (cond
           resp resp

           (> (System/currentTimeMillis) timeout-time)
           (ex-info (format "Leader not yet established and timeout of %s reached. Polled raft state %s times." timeout retries)
                    {:status 400 :error :db/leader-timeout})

           :else
           (do
             (async/<! (async/timeout 100))
             (recur (inc retries)))))))))

(defn is-leader?-async
  [raft]
  (async/go
    (let [leader (async/<! (leader-async raft))]
      (if (instance? Throwable leader)
        leader
        (= (:this-server raft) leader)))))


(defn is-leader?
  [raft]
  (let [leader? (async/<!! (is-leader?-async raft))]
    (if (instance? Throwable leader?)
      (throw leader?)
      leader?)))

(defn state
  [raft]
  (let [state (async/<!! (view-raft-state-async raft))]
    (if (instance? Throwable state)
      (throw state)
      state)))


;; TODO configurable timeout
(defn new-entry-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group entry] (new-entry-async group entry 5000))
  ([group entry timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (new-entry-async group entry timeout-ms callback)
     resp-chan))
  ([group entry timeout-ms callback]
   (async/go (let [raft'  (:raft group)
                 leader (async/<! (leader-async group))]
             (if (= (:this-server raft') leader)
               (raft/new-entry raft' entry callback timeout-ms)
               (let [id           (str (UUID/randomUUID))
                     command-data {:id id :entry entry}]
                 ;; since not leader, register entry id locally and will receive callback when committed to state machine
                 (raft/register-callback raft' id timeout-ms callback)
                 ;; send command to leader
                 (send-rpc raft' leader :new-command command-data nil)))))))

(defn add-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group newServer] (add-server-async group newServer 5000))
  ([group newServer timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (add-server-async group newServer timeout-ms callback)
     resp-chan))
  ([group newServer timeout-ms callback]
   (async/go (let [raft'  (:raft group)
                 leader (async/<! (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:add-server [id newServer] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :add-server [id newServer] nil)))))))

(defn remove-server-async
  "Sends a command to the leader. If no callback provided, returns a core async promise channel
  that will eventually contain a response."
  ([group server] (remove-server-async group server 5000))
  ([group server timeout-ms]
   (let [resp-chan (async/promise-chan)
         callback  (fn [resp]
                     (if (nil? resp)
                       (async/close! resp-chan)
                       (async/put! resp-chan resp)))]
     (remove-server-async group server timeout-ms callback)
     resp-chan))
  ([group server timeout-ms callback]
   (async/go (let [raft'  (:raft group)
                 leader (async/<! (leader-async group))
                 id     (str (UUID/randomUUID))]
             (if (= (:this-server raft') leader)
               (let [command-chan (-> group :command-chan)]
                 (async/put! command-chan [:remove-server [id server] callback]))
               (do (raft/register-callback raft' id timeout-ms callback)
                   ;; send command to leader
                   (send-rpc raft' leader :remove-server [id server] nil)))))))

(defn local-state
  "Returns local, current state from state machine"
  [raft]
  @(:state-atom raft))


(defn launch-raft-server
  [server-configs this-server raft-configs]
  (let [join?                  (:join? raft-configs)
        server-duplicates?    (not= (count server-configs) (count (into #{} (map :server-id server-configs))))
        _                     (when server-duplicates?
                                (throw (ex-info (str "There appear to be duplicates in the group servers configuration: "
                                                     (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        this-server-cfg       (some #(when (= this-server (:server-id %)) %) server-configs)
        _                     (when-not this-server-cfg
                                (throw (ex-info (str "This server: " (pr-str this-server) " has to be included in the group
                                server configuration." (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        raft-servers          (->> server-configs           ;; ensure unique
                                   (mapv :server-id server-configs)
                                   (into #{})
                                   (into []))
        server-duplicates?    (not= (count server-configs) (count raft-servers))
        _                     (when server-duplicates?
                                (throw (ex-info (str "There appear to be duplicates in the group servers configuration: "
                                                     (pr-str server-configs))
                                                {:status 400 :error :db/invalid-configuration})))
        raft-initialized-chan (async/promise-chan)
        leader-change-fn      (:leader-change-fn raft-configs)
        leader-change-fn*     (fn [change-map]
                                (let [{:keys [new-raft-state old-raft-state]} change-map]
                                  (log/info "Ledger group leader change:" (dissoc change-map :key :new-raft-state :old-raft-state))
                                  (log/debug "Old raft state: \n" (pr-str old-raft-state) "\n"
                                             "New raft state: \n" (pr-str new-raft-state))
                                  (when (not (nil? new-raft-state))
                                    (cond (and join? (not (nil? (:leader new-raft-state)))
                                               (not= this-server (:leader new-raft-state)))
                                          (async/put! raft-initialized-chan :follower)

                                          join?
                                          true

                                          (= this-server (:leader new-raft-state))
                                          (async/put! raft-initialized-chan :leader)

                                          :else
                                          (async/put! raft-initialized-chan :follower)))
                                  (when (fn? leader-change-fn)
                                    (leader-change-fn change-map))))
        raft-instance         (start-instance (merge raft-configs
                                                     {:port             (:port this-server-cfg)
                                                      :servers          raft-servers
                                                      :this-server      this-server
                                                      :leader-change-fn leader-change-fn*}))
        close-fn              (fn []
                                ;; close raft
                                (raft/close (:raft raft-instance))
                                ;; Unregister state-change-fns
                                (unregister-all-state-change-fn)
                                ;; close any open connections
                                (ftcp/close-all-connections this-server)
                                ;; close tcp server
                                ((:server-shutdown raft-instance)))]

    ;; we need a single duplex connection to each server.
    ;; TODO - Need slightly more complicated handling. If a server joins, close, and tries to restart with join = false, will fail
    (if join?
      ;; If joining an existing network, connects to all other servers
      (let [connect-servers (filter #(not= (:server-id this-server) %) server-configs)
            handler-fn      (partial message-consume (:raft raft-instance))]
        (doseq [connect-to connect-servers]
          (ftcp/launch-client-connection this-server-cfg connect-to handler-fn)))

      ;; simple rule (for now) is we connect to servers whose id is > (lexical sort) than our own
      (let [connect-servers (filter #(> 0 (compare this-server (:server-id %))) server-configs)
                    handler-fn      (partial message-consume (:raft raft-instance) )]
                (doseq [connect-to connect-servers]
                  (ftcp/launch-client-connection this-server-cfg connect-to handler-fn))))


    (-> (assoc raft-instance :raft-initialized raft-initialized-chan
                             :close close-fn))))


(defn stop-webserver
  []
  (->> (:webserver system)
       .close))

(defn stop
  []
  (do (stop-webserver) ((-> system :raft :close))))

(defn parse
  [x]
  (-> (cond (string? x) x
            (instance? ByteArrayInputStream x) (slurp x)
            (instance? InputStream x) (slurp x)
            :else (throw (ex-info (str "json parse error, unknown input type: " (pr-str (type x)))
                                  {:status 500 :error :db/unexpected-error})))
      (cjson/decode true)))

(defn handler
  []
  (fn [request]
    (let [deferred (d/deferred)]
      (async/go
        (try (if (= "close" (subs (:uri request) 1))
               (stop)
               (let [body   (-> request :body parse)
                     entry  (vec body)
                     body'  (async/<! (new-entry-async (:raft system) entry))
                     body'' (-> body' cjson/encode
                                (.getBytes "UTF-8"))]
                 (d/success! deferred {:status 200
                                       :body   body''})))
             (catch Exception e
               (d/error! deferred e))))
      deferred)))

(defn start-webserver
  [port]
  (http/start-server (handler) {:port port}))

(defn parse-provided-server-configs
  "Servers provided in the format:
  1@localhost:8080,2@localhost:8081"
  [node servers]
  (let [servers-vec   (str/split servers #",")
        server-config (map (fn [server]
                             (let [[id loc] (str/split server #"@")
                                   [host port] (str/split loc #":")]
                               (hash-map :server-id id
                                         :host host
                                         :port (parse port))))
                           servers-vec)
        this-port (some #(when (= node (:server-id %)) (:port %)) server-config)]
    [this-port server-config]))

(defn get-localhost-server-configs
  [node total]
  (let [port           (+ 9790 (dec node))
        server-configs (map #(hash-map :server-id (str (inc %))
                                       :host "localhost"
                                       :port (+ 9790 %)) (range 0 total))]
    [port server-configs]))

(defn start
  "Takes a node number - 1, 2, 3, 4, 5. And a total number of nodes"
   [node total]
  (let [node           (if (int? node) node (parse node))
        webserver-port (+ 8080 (dec node))
        this-server     (str node)
        [port server-configs] (cond (int? total)
                               (get-localhost-server-configs node total)

                               (try (parse total)
                                  (catch Exception e nil))
                               (get-localhost-server-configs node (parse total))

                               :else
                               (parse-provided-server-configs node total))
        raft-configs   {:port               port
                        :log-directory      (str "data/" node "/log")
                        :timeout-ms         1500
                        :heartbeat-ms       500
                        :retain-logs        5
                        :snapshot-threshold 200
                        :join?              false
                        :catch-up-rounds    10
                        :private-keys       ""
                        :open-api           true}
        raft           (launch-raft-server server-configs this-server raft-configs)
        webserver      (start-webserver webserver-port)
        system         {:raft raft :webserver webserver}
        _              (log/info "SYSTEM STARTED!")]
    (alter-var-root #'system (constantly system))))

(defn -main
  [& args]
  (apply start args))


(comment

  (start 2 2)

  system

  (local-state (:raft system))

  (def raft (:raft system))
  (def entry ["write" "a" "b"])
  (def entry-2 ["cas" "a" "b" "c"])
  (def entry-3 ["read" "a"])

  (def res (new-entry-async raft entry))

  (async/<!! res)

  )