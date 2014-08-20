(in-package :cta-frequency-server)

(hunchentoot:define-easy-handler (average-intervals :uri "/average-intervals")
    ((stop :parameter-type 'integer)
     route
     direction
     (earliest-date :parameter-type 'local-time:parse-timestring)
     (latest-date :parameter-type 'local-time:parse-timestring)
     (earliest-tod :parameter-type 'local-time:parse-timestring)
     (latest-tod :parameter-type 'local-time:parse-timestring)
     (dow :parameter-type '(list integer)))
  (pomo:with-connection *database-connection-spec*
    (find-average-interval :stop stop
                           :route route
                           :direction direction
                           :earliest-date earliest-date
                           :latest-date latest-date
                           :earliest-tod earliest-tod
                           :latest-tod latest-tod
                           :dow dow)))
