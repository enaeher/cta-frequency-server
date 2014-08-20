(in-package :cta-frequency-server)

(s-sql:enable-s-sql-syntax)

(defun %prepare-where-clause (&key stop route direction earliest-date latest-date earliest-tod latest-tod dow)
  (when (or stop route direction earliest-date latest-date earliest-tod latest-tod dow)
    `(:where
      (:and
       ,@(when stop `((:= 'stop ,stop)))
       ,@(when route `((:= 'route ,route)))
       ,@(when direction `((:= 'direction ,direction)))
       ,@(when dow `((:in (:date_part "dow" 'stop-time) (:set ,@dow))))
       ,@(cond ((and earliest-date latest-date)
                `((:between 'stop-time (:type ,earliest-date :date) (:type ,latest-date :date))))
               (earliest-date
                `((:>= 'stop-time (:type ,earliest-date :date))))
               (latest-date
                `((:<= 'stop-time (:type ,latest-date :date)))))
       ,@(cond ((and earliest-tod latest-tod)
                `((:between (:type 'stop-time :time)
                            (:type ,earliest-tod :time)
                            (:type ,latest-tod :time))))
               (earliest-tod
                `((:>= (:type 'stop-time :time)
                       ;; a nasty hack, but we cannot apparently
                       ;; directly cast the local-time string
                       ;; representation of a timestamp to a time
                       ;; without first casting it to a Postgres
                       ;; timestamp
                       (:type (:type ,earliest-tod :timestamp) :time))))
               (latest-tod
                `((:<=  (:type 'stop-time :time)
                        (:type (:type ,latest-tod :timestamp) :time)))))))))

(defun %prepare-query (&key stop route direction earliest-date latest-date earliest-tod latest-tod dow)
  `(:select (:type (/ (:floor (:date_part "epoch" (:avg 'interval))) 60) :integer) ;; get the interval as an integer number of minutes
            'route 'direction 'name (:type (:ST_AsGeoJSON 'stop-location) :json)
            :from
            (:as
             (:select
              (:as (:- 'stop-time (:over (:lag 'stop-time) 'w)) 'interval)
              'route
              'direction
              'name
              'stop-location
              :from 'stop-event
              :inner-join 'stop
              :on (:= 'stop-event.stop 'stop.id)
              ,@(%prepare-where-clause :stop stop :route route :direction direction
                                       :earliest-date earliest-date :latest-date latest-date
                                       :earliest-tod earliest-tod :latest-tod latest-tod :dow dow)
              :window (:as 'w (:partition-by 'route 'direction 'stop :order-by 'stop-time)))
             'all-intervals)
            :where (:not-null 'interval)
            :group-by 'route 'direction 'name 'stop-location))

(defun average-interval-to-json (average-interval s)
  (destructuring-bind (interval route direction stop location)
      average-interval
    (json:with-object (s)
      (json:encode-object-member 'interval interval s)
      (json:encode-object-member 'route route s)
      (json:encode-object-member 'direction direction s)
      (json:encode-object-member 'stop stop s)
      ;; location is already JSON, so just write it directly to the stream
      (json:as-object-member ('location s)
        (princ location s)))))

(defun average-intervals-to-json (average-intervals stream)
  (json:with-array (stream)
    (dolist (interval average-intervals)
      (json:as-array-member (stream)
        (average-interval-to-json interval stream)))))

(defun find-average-interval (&key
                                stop ;; the stop ID, an integer
                                route ;; the route ID, a string
                                direction ;; the direction, a string
                                earliest-date ;; the earliest date, a local-time:date
                                latest-date ;; the latest date, a local-time:date
                                earliest-tod ;; the earliest time of day, a local-time:timestamp (the date part is ignored)
                                latest-tod ;; the latest time of day, a local-time:timestamp (the date part is ignored)
                                dow ;; the days of the week to include, a list of integers (Sunday = 0)
                                )  
  "Returns an average time between buses (along with route,
direction, name, and stop location) for each route/direction/stop
combination which matches the specified filter criteria.

Arguments which are omitted are ignored. Omitting all arguments
returns an average time for all route/direction/stop combinations in
the database, for all stop events in the database.

Filter criteria fall into two categories:

STOP, ROUTE, and DIRECTION, if provided, narrow the set of
stops/routes/directions for which average stop intervals will be
returned. Using these filters will reduce the number of rows returned.

EARLIEST-DATE, LATEST-DATE, EARLIEST-TOD, LATEST-TOD, and DOW, if
provided, chronologically narrow the list of stop events which will be
considered in calculating the average interval. Using these filters
will not affect the number of rows returned, but will affect the
reported average time between buses."
  (let ((query (%prepare-query :stop stop :route route :direction direction
                               :earliest-date earliest-date :latest-date latest-date
                               :earliest-tod earliest-tod :latest-tod latest-tod :dow dow)))
    (with-output-to-string (s)
      (average-intervals-to-json (pomo:query (s-sql:sql-compile query)) s))))
