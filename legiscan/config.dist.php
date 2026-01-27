<?php
;// vim: ft=dosini:
;// Just in case the user is not protecting this file properly,
;// create a valid php file that also parses as a INI file
;die();
;/* START OF REAL CONFIGURATION

;;
;; LegiScan API configuration file, lines beginning with ';' are comments
;;

;;
;; PDO DSN for database server, examples:
;;
;; MySQL tcp port
;;   dsn = "mysql:host=localhost;port=3306;dbname=legiscan_api"
;; MySQL unix socket
;;   dsn = "mysql:unix_socket=/var/run/mysqld/mysqld.sock;dbname=legiscan_api"
;;
;; PostgreSQL
;;   dsn = "pgsql:host=localhost;port=5432;dbname=legiscan_api"
;;
;; MySQL and PostgreSQL officially supported databases, MSSQL may need tweaking
;;
;; MS SQL Server
;;   dsn = "sqlsrv:Server=localhost,1433;Database=legiscan_api"
;;
[database]
dsn = "mysql:host=localhost;port=3306;dbname=legiscan_api"
;; User name used to connect to database
db_user = legiscan_api
;; Password for db_user to connect with database
db_pass = 

;; Massage reserved date 0000-00-00 to relaxed NULL value for non MySQL databases
;; Set to 1 for PostgresSQL and MS SQL Server
massage_dates = 0


;;
;; Memcache is optional, though can greatly increase performance and
;; lessen database strain. If not set, a simple PHP static memory cache
;; will be transparently used instead
;;
[memory_cache]
;; Use persistant external memcached server, 0 or 1 and specify host/port
use_memcached = 0
;; Memcache/Memcached host or socket
memcache_host = localhost
;; Memcache/Memcached port (0 with socket hose)
memcache_port = 11211


;; --------------------------------------------------------------------
;; General LegiScan settings
;; --------------------------------------------------------------------
[legiscan]
;; LegiScan API Key
api_key = 

;; Auth token for Push from API Control Panel, if empty/unset
;; authorization header token check will be skipped
api_auth_token = 

;; Email address to send reports and alerts, can be left blank
;; and emails will be skipped (presumes working mail() function)
email = 

;; If push is configured for cooked application/x-www-form-urlencoded
;; encoded pushes assigned to a form variable set that field name here
;push_form_var = 


;; Directory locations, can absolute path or relative path

;; Location of API cache directory
api_cache = "./cache/api"

;; Location of API document directory for texts, amendments, supplements
doc_cache = "./cache/doc"

;; Location of where log files are stored
log_dir = "./log"


;; Wants and needs for further records/details beyond the master
;; bill payload to be pulled (votes) or stored locally (documents)
;;
;; Request legislator detail records for each vote, 0 or 1
want_vote_details = 1

;; Request copy of HTML/PDF bill texts, 0 or 1
want_bill_text = 0

;; Request copy of HTML/PDF bill amendments, 0 or 1
want_amendment = 0

;; Request copy of HTML/PDF bill supplements, 0 or 1
want_supplement = 0

;; Prefer the PDF version of a document when multiple formats exist,
;; if PDF does not exist will receive other format such as HTML, 0 or 1
prefer_pdf = 0


;; This option will cause the system to "signal" a middleware monitor that
;; a bill, text, etc has been updated so that it can be further processed.
;;
;; Store in DB table ls_signal with object, id, processed flag
;middleware_signal = table
;; Store in ./signal as filenames in form object.id.timestamp
;middleware_signal = directory


;; --------------------------------------------------------------------
;; LegiScan Pull Daemon Control
;; --------------------------------------------------------------------

[legiscand]
;; Update Type controls what qualifies a bill for importing and monitoring.
;; Uncomment one update_type as needed
;;
;; Replicate all bills from the ls_monitor table
;update_type = monitored
;; Fully replicate all bills from the states[] list
;update_type = state
;; Import bills based on national searches[] list and relevance scores
;update_type = search
;; Import bills running searches[] list against each states[] list
;update_type = state_search

;; If set to 1, bill_ids from ls_ignore table will be ignored for importing
use_ignore_table = 0

;; Update interval in seconds, values between 3600 (1 hour) and 86400 (1 day)
;; Suggested default 3 hours (10800) 
interval = 10800

;; Default minimum relevance score a bill must have to qualify
;; to be imported to the database from a search query, can also
;; be set per search (see below)
relevance = 50

;; States to synchronize via pull for update_type = state | state_search and
;; from bulk update from legiscan-bulk.php
;; Add multiple lines for each state abbreviation as needed, ALL will expand
;; to all states and Congress
;;
;; NOTE: strongly recommended to seed with datasets to save onboarding requests
;;
;states[] = CA
;states[] = US

;; Years to synchronize via dataset update method from legiscan-bulk.php
;; Add multiple lines for each year as needed, CURRENT will be replaced
;; with the current calendar year, ALL will expand to all available years
years[] = CURRENT
;years[] = 2021
;years[] = 2020

;; Searches to synchronize via pull for update_type = search | state_search
;;
;; A search can be written as SCORE|SEARCH to override the default relevance
;; cutoff for that particular search, such as:
;;   searches[] = "42|hemp OR cannabis OR marijuana"
;;
;; A search can be written as STATE|SEARCH to override the state setting in
;; search and state_search modes:
;;   searches[] = "NY|charter ADJ schools"
;;
;; A search can be written as STATE,SCORE|SEARCH to override both state and
;; relevance settings on a per search basis as in:
;;   searches[] = "CA,60|vaccination"
;;
;; Also note that if you are using quoted phrases, all inside quotes need
;; to be 'escaped' with '\', eg:
;;   searches[] = "\"national popular vote\""
;;   
;searches[] = "75|vaccination"
;searches[] = "\"national popular vote\""
;searches[] = "gender AND bathroom"
;searches[] = "CA,60|vaccination"
;searches[] = "NY|charter ADJ schools"
;searches[] = "42|hemp OR cannabis OR marijuana"
;searches[] = "abortion OR (pregnancy NEAR termination)"



;; END OF CONFIGURATION -- DO NOT MODIFY BELOW
;*/
;?>
