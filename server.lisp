(in-package :cta-frequency-server)

(defparameter *server* (make-instance 'hunchentoot:easy-acceptor :port *port*))
(defparameter hunchentoot:*default-content-type* "application/json")

(defun start ()
  (hunchentoot:start *server*))

(defun stop ()
  (hunchentoot:start *server*))
