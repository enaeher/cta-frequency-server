(in-package :cta-frequency-server)

(s-sql:enable-s-sql-syntax)

(defun %prepare-where-clause (&key stop route direction earliest-date latest-date earliest-hour latest-hour dow)
  (when (or stop route direction earliest-date latest-date earliest-hour latest-hour dow)
    `(:where
      (:and
       ,@(when stop `((:= 'stop ,stop)))
       ,@(when route `((:= 'route ,route)))
       ,@(when direction `((:= 'direction ,direction)))
       ,@(when dow `((:in (:date_part "dow" 'interval-end) (:set ,@dow))))
       ,@(cond ((and earliest-date latest-date)
                `((:between 'stop-time (:type ,earliest-date :date) (:type ,latest-date :date))))
               (earliest-date
                `((:>= 'stop-time (:type ,earliest-date :date))))
               (latest-date
                `((:<= 'stop-time (:type ,latest-date :date)))))
       ,@(cond ((and earliest-hour latest-hour)
                `((:between (:date_part "hour" 'interval-end)
                            ,earliest-hour
                            ,latest-hour)))
               (earliest-hour
                `((:>= (:date_part "hour" 'interval-end) ,earliest-hour)))
               (latest-hour
                `((:<=  (:date_part "hour" 'interval-end) ,latest-hour))))))))

(defun %prepare-query (&key stop route direction earliest-date latest-date earliest-hour latest-hour
                         dow maximum-average-interval minimum-average-interval)
  `(:with (:as 'averages
               (:select (:as (/ (:avg (:- (:date-part "epoch" 'interval-end)
                                          (:date-part "epoch" 'interval-start)))
                                60) 'interval) ;; get the interval as a number of minutes
                        'stop-route-direction
                        :from 'stop-interval
                        ,@(when (or stop route direction)
                                `(:inner-join 'stop-route-direction :on (:= 'stop-interval.stop-route-direction 'stop-route-direction.id)))
                        ,@(when route
                                `(:inner-join 'route :on (:= 'stop-route-direction.route route.id)))
                        ,@(when stop
                                `(:inner-join 'stop :on (:= 'stop-route-direction.stop stop.id)))
                        ,@(%prepare-where-clause :stop stop :route route :direction direction
                                                 :earliest-date earliest-date :latest-date latest-date
                                                 :earliest-hour earliest-hour :latest-hour latest-hour :dow dow)
                        :group-by 'stop-route-direction))
          (:select 'interval 'route 'route.name 'direction 'stop.name (:type (:ST_AsGeoJSON 'stop-location) :json)
                   :from 'averages
                   :inner-join 'stop-route-direction :on (:= 'averages.stop-route-direction 'stop-route-direction.id)
                   :inner-join 'stop :on (:= 'stop-route-direction.stop 'stop.id)
                   :inner-join 'route :on (:= 'stop-route-direction.route 'route.id)
                   ,@(when (or maximum-average-interval minimum-average-interval)
                           `(:where ,(cond ((and maximum-average-interval minimum-average-interval)
                                            `(:between 'interval ,minimum-average-interval ,maximum-average-interval))
                                           (maximum-average-interval
                                            `(:<= 'interval ,maximum-average-interval))
                                           (minimum-average-interval
                                            `(:>= 'interval ,minimum-average-interval))))))))

(defun average-interval-to-json (average-interval s)
  (destructuring-bind (interval route route-name direction stop location)
      average-interval
    (json:with-object (s)
      (json:encode-object-member 'type "Feature" s)
      (json:as-object-member ('properties s)
        (json:with-object (s)
          (json:encode-object-member 'interval interval s)
          (json:encode-object-member 'route route s)
          (json:encode-object-member 'route-name route-name s)
          (json:encode-object-member 'direction direction s)
          (json:encode-object-member 'stop stop s)))
      ;; location is already JSON, so just write it directly to the stream
      (json:as-object-member ('geometry s)
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
                                earliest-hour ;; the earliest hour of the day, an integer between 0 and 23
                                latest-hour ;; the latest hour of the day                                
                                dow ;; the days of the week to include, a list of integers (Sunday = 0)
                                maximum-average-interval ;; the the maximum average interval (stops which
                                                         ;; exceed this interval will be excluded from the results)
                                minimum-average-interval ;; you can probably figure this one out
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

EARLIEST-DATE, LATEST-DATE, EARLIEST-HOUR, LATEST-HOUR, and DOW, if
provided, chronologically narrow the list of stop events which will be
considered in calculating the average interval. Using these filters
will not affect the number of rows returned, but will affect the
reported average time between buses."
  (let ((query (%prepare-query :stop stop :route route :direction direction
                               :earliest-date earliest-date :latest-date latest-date
                               :earliest-hour earliest-hour :latest-hour latest-hour :dow dow
                               :maximum-average-interval maximum-average-interval
                               :minimum-average-interval minimum-average-interval)))
    (pomo:query (s-sql:sql-compile query))))
