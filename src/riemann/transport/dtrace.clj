(ns riemann.transport.dtrace
  "Generates riemann events from dtrace probes."
  (:import [org.opensolaris.os.dtrace DataEvent
                                      ConsumerAdapter
                                      LocalConsumer]
           [java.io File])
  (:use [clojure.tools.logging :only [info warn]]
        [riemann.service :only [Service ServiceEquiv]]
        [riemann.time :only [unix-time]]
        [riemann.transport :only [handle]]))


(defn dtrace-adapter
  [core handler]
  ; Translate Dtrace calls to events
  (proxy [ConsumerAdapter]
    []
    (dataReceived [^DataEvent e]
      (handle core (handler e)))))

(defn dtrace-handler
  [^DataEvent e]
  ({:service (.getSource e)
    :metric (.getProveData e)
    :time unix-time}))

(defrecord DtraceServer [^File file
                         core
                         receive
                         killer]
  ; core is a reference to a core

  ServiceEquiv
  (equiv? [this other]
          (and (instance? DtraceServer other)
               (= file (:file other))))

  Service
  (conflict? [this other]
             (and (instance? DtraceServer other)
                  (= file (:file other))))

  (reload! [this new-core]
           (reset! core new-core))

  (start! [this]
            (when-not @killer
              (let [consumer (LocalConsumer. )
                    adapter (dtrace-adapter receive)]
                 (info "DtraceServer on " file " starting")
                 (reset! killer
                   (fn killer []
                     (.close consumer)
                     (info "Closing DtraceServer on " file)))
                 (.open consumer)
                 (.compile consumer file)
                 (.enable consumer)
                 (.go consumer))))

  (stop! [this]
       (when @killer
         (@killer)
         (reset! killer nil))))


(defn dtrace-server
  "A server which will listen to dtrace events. Options:
  :file             The dtrace definitions.
  :receiver         A Clojure which will process the Dtrace event
  :core             An atom used to track the active core for this server"
  ([]
   (dtrace-server {}))
  ([opts]
   (let [core     (get opts :core (atom nil))
         file     (get opts :file nil)
         receive  (get opts :receive nil)]

       (DtraceServer. file core receive (atom nil)))))
