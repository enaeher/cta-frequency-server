(defsystem :cta-frequency-server
  :license "Public Domain"
  :author "Eli Naeher"
  :version (:read-file-form "VERSION")
  :depends-on (:alexandria :local-time :cl-postgres+local-time :postmodern :hunchentoot :cl-json)
  :components ((:file "package")
               (:file "average-intervals" :depends-on ("package"))
               (:file "server" :depends-on ("package"))
               (:file "routes" :depends-on ("server"))))
