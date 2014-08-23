(in-package :cta-frequency-server)

(hunchentoot:define-easy-handler (average-intervals :uri "/average-intervals")
    ((stop :parameter-type 'integer)
     route
     direction
     (earliest-date :parameter-type 'local-time:parse-timestring)
     (latest-date :parameter-type 'local-time:parse-timestring)
     (earliest-hour :parameter-type 'integer)
     (latest-hour :parameter-type 'integer)
     (dow :parameter-type '(list integer)))
  (pomo:with-connection *database-connection-spec*
    (let ((stream (flex:make-flexi-stream (hunchentoot:send-headers) :external-format (flex:make-external-format :utf-8 :eol-style :lf))))
      (average-intervals-to-json (find-average-interval :stop stop
                                                        :route route
                                                        :direction direction
                                                        :earliest-date earliest-date
                                                        :latest-date latest-date
                                                        :earliest-hour earliest-hour
                                                        :latest-hour latest-hour
                                                        :dow dow)
                                 stream))))

(hunchentoot:define-easy-handler (routes :uri "/routes")
    nil
  (pomo:with-connection *database-connection-spec*
    (let ((stream (flex:make-flexi-stream (hunchentoot:send-headers) :external-format (flex:make-external-format :utf-8 :eol-style :lf))))
      (cl-json:with-array (stream)
        (pomo:with-connection *database-connection-spec*
          (dolist (row (pomo:query (:select '* :from 'route) :plists))
            (cl-json:as-array-member (stream)
              (cl-json:encode-json-plist row stream))))))))
