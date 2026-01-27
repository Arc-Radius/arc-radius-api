<?php
/**
 * LegiScan Client API
 * 
 * A simple implementation of the LegiScan API Pull, Push and Bulk classes
 *
 * @package LegiScan
 * @author LegiScan API Team <api@legiscan.com>
 * @license https://opensource.org/licenses/BSD-2-Clause
 * @copyright 2010-2020 LegiScan LLC
 * 
 */

require_once(__DIR__ . '/' . 'common.php');

// {{{ Exceptions
/**
 * Exception class for API general errors
 * @package LegiScan\Exceptions
 */
class APIException extends Exception { }

/**
 * Exception class for API missing file / bad permissions
 * @package LegiScan\Exceptions
 */
class APIAccessException extends Exception { }

/**
 * Exception class for API status errors
 * @package LegiScan\Exceptions
 */
class APIStatusException extends Exception { }

/**
 * Exception class for API authorization token errors
 * @package LegiScan\Exceptions
 */
class APIAuthTokenException extends Exception { }
// }}}

// {{{ LegiScan Class
/**
 * A generic holder for some core static functions
 *
 * Non-inherited class primarily for static calls to pull configuration values, write log messages,
 * send mail alerts and hold constants. You will likely find {@see LegiScan_Pull} and
 * {@see LegiScan_Process} of much greater interest.
 *
 * @package LegiScan\Utility
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/legiscan
 *
 */
class LegiScan
{
	// {{{ Class Constants
	/**
	 * Version number of the LegiScan API Client
	 */
	const VERSION = '1.4.1';

	/**
	 * Version number of the LegiScan API database schema
	 */
	const SCHEMA_VERSION = 9;

	/**
	 * Response string for a successful response
	 */
	const API_OK = 'OK';

	/**
	 * Response string for a failure response
	 */
	const API_ERROR = 'ERROR';

	/**
	 * MasterList/Search import control, do nothing
	 */
	const IMPORT_NONE = 0;

	/**
	 * MasterList/Search import control, only new bills
	 */
	const IMPORT_NEW = 1;

	/**
	 * MasterList/Search import control, only changed bills
	 */
	const IMPORT_CHANGED = 2;

	/**
	 * MasterList/Search import control, all bills
	 */
	const IMPORT_ALL = 3;

	/**
	 * setMonitor stance, watch bill
	 */
	const STANCE_WATCH = 0;

	/**
	 * setMonitor stance, support bill
	 */
	const STANCE_SUPPORT = 1;

	/**
	 * setMonitor stance, oppose bill
	 */
	const STANCE_OPPOSE = 2;
	// }}}

	// {{{ getConfig()
	/**
	 * Get the configuation array or a specific key from it
	 *
	 * @param string|null $key
	 *   (**OPTIONAL**) Return `$key` from config array
     *
	 * @param mixed|null $default
	 *   (**OPTIONAL**) If `$key` does not exist return `$default`
	 *
	 * @return mixed
	 *   Either the configuration array or a specific `$key` value
	 */
	static function getConfig($key = null, $default = null)
	{
		static $config = array();

		if (empty($config))
			$config = parse_ini_file(realpath(__DIR__ . '/' . 'config.php'));

		if ($key !== null)
		{
			// Look for a specific key
			if (isset($config[$key]))
			{
				return $config[$key];
			}
			else
			{
				if ($default !== null)
					return $default;
				else
					return null;
			}
		}
		else
		{
			// Return the entire config array
			return $config;
		}
	}
	// }}}

	// {{{ fileLog()
	/**
	 * Write log message to file located at {log_dir}/legiscan.log
	 *
	 * @throws APIAccessException
	 *
	 * @param string $msg
	 *   The log message to be written
	 *
	 * @param string $file
	 *   (**OPTIONAL**) If specified write log to {log_dir}/$file
	 *
	 */
	static function fileLog($msg, $file = null)
	{
		static $log_dir;

		if (!isset($log_dir))
		{
			$log_dir = LegiScan::getConfig('log_dir');
			if ($log_dir[0] != '/' || $log_dir[1] != ':')
				$log_dir = realpath(__DIR__ . '/' . $log_dir);
			if (!is_dir($log_dir))
				throw new APIAccessException("Log directory does not exist: " . LegiScan::getConfig('log_dir'));
		}

		$log_file = $log_dir . '/' . 'legiscan.log';
		if ($file !== null)
			$log_file = $log_dir . '/' . $file;

		if (($fd = @fopen($log_file, 'a+')) !== false)
		{
			$msg = rtrim($msg);
			$ts = strftime('[%Y/%m/%d %H:%M:%S]');
			fwrite($fd, $ts . ' ' . $msg . "\n");
			fclose($fd);
		}
		else
		{
			throw new APIAccessException("Cannot open log file for writing: $log_file");
		}
	}
	// }}}

	// {{{ sendMail()
	/**
	 * Send an email to the address specified in config.php, throttled
	 * to 1 msg/5 min (overflow sent to fileLog)
	 *
	 * @param string $subject
	 *   The email subject
	 *
	 * @param string $body
	 *   The email body
	 *
	 */
	static function sendMail($subject, $body)
	{
		static $last_sent;
		static $throttle_warning = false;

		if (!isset($last_sent))
		{
			$last_sent = time();
			$throttled = false;
		}
		else
		{
			$throttled = (bool) ((time() - $last_sent) <= 300);
		}

		$email = LegiScan::getConfig('email');
		$header = "From: LegiScan API <$email>";

		if ($email && $email === filter_var($email, FILTER_VALIDATE_EMAIL) && !$throttled)
		{
			mail($email, $subject, $body, $header);
			$last_sent = time();
			$throttle_warning = false;
		}
		elseif ($throttled)
		{
			if (!$throttle_warning)
			{
				$body = "Mail has been throttled for the next 5 minutes, please see legiscan.log for more messages\n\n$body";
				mail($email, "Throttled: $subject", $body, $header);
				$throttle_warning = true;
			}
			else
			{
				LegiScan::fileLog("Mail throttled [$subject] " . str_replace("\n", '\\n', $body));
			}
		}
	}
	// }}}

	// {{{ getVersion()
	/**
	 * Get the LegiScan version number
	 *
	 * @return string Version number
	 */
	static function getVersion()
	{
		return self::VERSION;
	}
	// }}}
}
// }}}

// {{{ LegiScan_Cache_File Class
/**
 * LegiScan API File Cache Class
 *
 * Basic file caching with expirations to cache API responses and
 * build permanent local bill text / document storage. To generate the
 * path fragment under each respective cache directories use 
 * {@link LegiScan_Pull::getCacheFilename()} or
 * {@link LegiScan_Process::getCacheFilename()}.
 *
 * ## API cache
 *
 * As a dynamic API cache this stores pull API requests and search
 * results in a local directory structure if there are no errors and
 * subsequent calls to the same API hook will replay.
 *
 * Directory structure where `{api_dir}` is from `LegiScan::getConfig('api_dir')`
 *
 * * `{api_dir}/amendment/AMENDMENT_ID.json`
 * * `{api_dir}/bill/BILL_ID.json`
 * * `{api_dir}/masterlist/SESSION_ID.json`
 * * `{api_dir}/people/PEOPLE_ID.json`
 * * `{api_dir}/rollcall/ROLL_CALL_ID.json`
 * * `{api_dir}/search/SEARCH_GUID.json`
 * * `{api_dir}/sessionlist/STATE.json`
 * * `{api_dir}/supplement/SUPPLEMENT_ID.json`
 * * `{api_dir}/text/TEXT_ID.json`
 *
 * ## Document cache
 *
 * As a static document cache this is the basis for local storage of
 * related documents (bill texts, amendments, supplements) as a well
 * structured archive that can be easily accessed by humans and automation.
 * 
 * Directory structure `TYPE/STATE/SESSION_ID/BILL_NUMBER/ID/MIME_EXT`
 * where `{doc_dir}` is from `LegiScan::getConfig('doc_dir')`
 *
 * * `{doc_dir}/amendment/tx/1478/hb1/3928402/pdf`
 * * `{doc_dir}/supplement/md/1412/sb493/56768/pdf`
 * * `{doc_dir}/text/ca/1400/sb1/1439083/html`
 *
 * @package LegiScan\Cache
 * @see LegiScan_Pull::getCacheFilename
 * @see LegiScan_Process::getCacheFilename
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_Cache_File
{
	// {{{ Class Constants
	/**
	 * Type - API (expiration)
	 */
	const TYPE_API = 1;
	/**
	 * Type - Document (permanent)
	 */
	const TYPE_DOC = 2;
	/**
	 * Default lifetime of API cache objects in seconds (60 minutes)
	 */
	const LIFETIME = 3600;
	// }}}

	// {{{ Class Variables
	/**
	 * Lifetime of API cache objects
	 */
	private $lifetime;
	/**
	 * Type of cache
	 */
	private $cache_type;
	/**
	 * Base directory for cached files
	 */
	private $cache_dir;
	// }}}

	// {{{ __construct()
	/**
	 * Class constructor
	 *
	 * @throws APIAccessException
	 *
	 * @param string $type Type of cache {api, doc}
	 * @param integer $lifetime Time in seconds for api cache lifetime
	 */
	function __construct($type, $lifetime = self::LIFETIME)
	{
		switch ($type)
		{
			case 'api':
				$api_dir = LegiScan::getConfig('api_cache');
				if ($api_dir[0] != '/' && $api_dir[1] != ':')
					$api_dir = realpath(__DIR__ . '/' . $api_dir);
				$this->cache_dir = $api_dir;
				if (!file_exists($this->cache_dir) || !is_dir($this->cache_dir))
					throw new APIAccessException("LegiScan_Cache(): Invalid api cache directory: " . $this->cache_dir);
				$this->cache_type = self::TYPE_API;
				$this->lifetime = $lifetime;
				break;
			case 'doc':
				$doc_dir = LegiScan::getConfig('doc_cache');
				if ($doc_dir[0] != '/' && $doc_dir[1] != ':')
					$doc_dir = realpath(__DIR__ . '/' . $doc_dir);
				$this->cache_dir = $doc_dir;
				if (!file_exists($this->cache_dir) || !is_dir($this->cache_dir))
					throw new APIAccessException("LegiScan_Cache(): Invalid document cache directory: " . $this->cache_dir);
				$this->cache_type = self::TYPE_DOC;
				$this->lifetime = null;
				break;
		}
	}
	// }}}

	// {{{ get()
	/**
	 * Get an entry from the cache, if operating as API cache also enfore expiration lifetime,
	 * otherwise treat as a permanent document archive
	 *
	 * @param string $cache_file
	 *   File location to retrieve, this may include additional directories under _cache\_dir_
	 *
	 * @param integer $lifetime
	 *   Max age in seconds that a cached file is valid
	 *
	 * @return mixed
	 *   The contents of the cache location or FALSE for missing/stale
	 */
	public function get($file, $lifetime = null)
	{
		$filename = $this->cache_dir . '/' . $file;

		if (!file_exists($filename))
			return false;

		if ($this->cache_type == self::TYPE_API)
		{
			if ($lifetime === null)
				$lifetime = $this->lifetime;

			if (filemtime($filename) > (time() - $lifetime))
				$data = file_get_contents($filename);
			else
				$data = false;
		}
		else
		{
			$data = file_get_contents($filename);
		}

		return $data;
	}
	// }}}

	// {{{ set()
	/**
	 * Put an entry into the cache, creating any nested directories as required
	 *
	 * @throws APIAccessException
	 *
	 * @param string $file
	 *   File location to store, this may include additional directories under _cache\_dir_
	 *
	 * @param string $data
	 *   Data to store in cache location
	 *
	 * @return boolean
	 *   Indicating the success/failure of storing the data
	 *
	 */
	public function set($file, $data)
	{
		$filename = $this->cache_dir . '/' . $file;
		$dirname = dirname($filename);

		// Check if the directory path exists
		if (!file_exists($dirname))
		{
			// Create it recursively
			if (!@mkdir($dirname, 0777, true))
				throw new APIAccessException("Cannot create directory $dirname");
		}

		if (@file_put_contents($filename, $data) !== false)
			return true;
		else
			throw new APIAccessException("Cannot write $filename");
	}
	// }}}

	// {{{ clean()
	/**
	 * Clean the API cache of stale entries
	 *
	 * @throws APIAccessException
	 *
	 * @return boolean
	 *   Indicating the success/failure of the cleanup
	 *
	 */
	public function clean($verbose = false)
	{
		$result = true;

		/**
		 * Directories under API cache root that we own 
		 * to avoid stepping on toes
		 *
		 * @var $subdirs
		 */
		$subdirs = array(
			'amendment',
			'datasetlist',
			'bill',
			'masterlist',
			'people',
			'rollcall',
			'search',
			'sessionlist',
			'supplement',
			'text',
		);

		$size = 0;
		$count = 0;

		if ($this->cache_type == self::TYPE_API)
		{
			$dir = $this->cache_dir;
			if (!($dh = opendir($dir))) {
				throw new Exception("clean(): Unable to open API cache directory $dir");
			}

			while (($file = readdir($dh)) !== false)
			{
				if ($file != '.' && $file != '..' && in_array($file, $subdirs))
				{
					$dir2 = $dir . '/' . $file;
					if (is_dir($dir2))
					{
						if (!($dh2 = opendir($dir2)))
							throw new APIAccessException("clean(): Unable to open API cache directory $dir2");

						while (($file2 = readdir($dh2)) !== false)
						{
							if ($file2 != '.' && $file2 != '..' && substr($file2, -4) == 'json')
							{
								$file2 = $dir2 . '/' . $file2;
								// files older than lifetime get deleted from cache
								if ((time() - @filemtime($file2)) > $this->lifetime)
								{
									if ($verbose) echo "Unlink $file2\n";
									$stat = stat($file2);
									$size += $stat['size'];
									$result = ($result and (@unlink($file2)));
									$count++;
								}
							}
						}
					}
					closedir($dh2);
				}
			}
			closedir($dh);
		}

		if ($verbose)
			echo number_format($size) . ' bytes cleaned in ' . number_format($count) . " files\n";

		return $result;
	}
	// }}}
}
// }}}

// {{{ LegiScan_Cache_Memory Class
/**
 * LegiScan API Memory Cache Class
 *
 * Basic singleton memory cache that offers a simple non-stateful memory cache or
 * interfaces with an external memcache server, either via Memcache or Memcached.
 *
 * Underutilized in this application, it currently caches simple `exists(ID)` style
 * lookups. Internally at LegiScan this saves 100-125 qps in production, though in 
 * this isolated API Client implementation will be around 5-10 qps.
 *
 * <code>
 * $memcache = LegiScan_Cache_Memory::getInstance();
 *
 * $key = "key-sample-1";
 *
 * $memcache->set($key, 12345);
 * $value = $memcache->get($key);
 *
 * var_dup($value);
 * </code>
 *
 * @package LegiScan\Cache
 * @see LegiScan_Process::checkExists
 * @link https://memcached.org/
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_Cache_Memory
{
	// {{{ Class Constants
	/**
	 * Default lifetime of API cache objects in seconds (60 minutes)
	 */
	const LIFETIME = 3600;
	// }}}

	// {{{ Class Variables
	/**
	 * Lifetime of cache objects
	 * @var integer
	 */
	private static $lifetime;
	/**
	 * Our cache array if no memcached server
	 * @var array
	 */
	private static $cache;
	/**
	 * Singleton class instance
	 * @var object
	 */
	private static $instance;
	// }}}

	// {{{ __construct()
	/**
	 * This is not the contructor you are looking for... Static Singleton
	 *
	 * @param integer $lifetime
	 *   Time in seconds for api cache lifetime
	 *
	 */
	private function __construct($lifetime)
	{
		self::$cache = array();
		self::$lifetime = $lifetime;
	}
	// }}}

	// {{{ getInstance()
	/**
	 * Get a instance of either internal memory cache or external memcached
	 *
	 * @param integer $lifetime
	 *   Lifetime of a cached value
	 *
	 */
	public static function getInstance($lifetime = self::LIFETIME)
	{
		if (!isset(self::$instance))
		{
			$use_memcache = strtolower(LegiScan::getConfig('use_memcached'));
			if ($use_memcache)
			{
				$memcache_host = LegiScan::getConfig('memcache_host');
				$memcache_port = LegiScan::getConfig('memcache_port');

				if (class_exists('Memcache', false))
				{
					self::$instance = new Memcache();
					if (!@self::$instance->connect($memcache_host, $memcache_port))
						throw new APIException('Could not connect via Memcache to ' . $memcache_host . ':' . $memcache_port);
				}
				elseif (class_exists('Memcached', false))
				{
					self::$instance = new Memcached();
					self::$instance->addServer($memcache_host, $memcache_port);

					// Validate since Memcached will not connect from pool until needed
					if (empty(self::$instance->getVersion()))
						throw new APIException('Could not connect via Memcached to ' . $memcache_host . ':' . $memcache_port);
				}
				else
				{
					throw new APIException('Missing Memcache/Memcached PHP extension for use_memory_cache = ' . $use_memcache);
				}
			}
			else
			{
				self::$instance = new LegiScan_Cache_Memory($lifetime);
			}
		}

		return self::$instance;
	}
	// }}}

	// {{{ get()
	/**
	 * Get an entry from the cache
	 *
	 * @param string $key
	 *   Cache key to retrieve
	 *
	 * @return mixed
	 *   The contents of the file or FALSE for missing/stale
	 */
	public function get($key)
	{
		$data = false;

		if (isset(self::$cache[$key]) && self::$cache[$key]['expires'] > time())
		{
			$data = self::$cache[$key]['data'];
		}

		return $data;
	}
	// }}}

	// {{{ set()
	/**
	 * Store an entry in the cache
	 *
	 * @param string $key
	 *   Cache key value
	 *
	 * @param string $data
	 *   Data to store under cache key
	 *
	 * @param string $lifetime
	 *   How long until the data expires, in seconds
	 *
	 */
	public function set($key, $data, $lifetime = self::LIFETIME)
	{
		self::$cache[$key] = array(
			'expires' => time() + $lifetime,
			'lifetime' => $lifetime,
			'data' => $data,
		);
	}
	// }}}

	// {{{ flush()
	/**
	 * Clear all items from cache
	 *
	 * @return boolean
	 *   Indicating the success/failure of the cleanup
	 *
	 */
	public function flush()
	{
		self::$cache = array();

		return true;
	}
	// }}}
}
// }}}

// {{{ LegiScan_Push Class
/**
 * LegiScan API Push Interface Class
 *
 * Framework to process and decode incoming payloads as the endpoint listener
 * in a Push API interface.
 *
 * @package LegiScan\API
 * @see LegiScan_Endpoint
 * @see LegiScan_Process
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/legiscan
 *
 */
class LegiScan_Push
{
	// {{{ Class Variables
	/**
	 * The decoded payload that was last processed through {@link processPush()}
	 *
	 * @var array
	 */
	protected $payload;

	/**
	 * The type of payload that was last processed through {@link processPush()}
	 *
	 * @var string
	 */
	protected $payload_type;

	/**
	 * Form variable name in the case of cooked x-www-form-urlencoded payload
	 *
	 * @var string
	 */
	protected $form_var;
	// }}}

	// {{{ __construct()
	/** 
	 * Class constructor
	 *
	 */
	function __construct()
	{
		$this->form_var = LegiScan::getConfig('push_form_var', 'UNKNOWN');
	}
	// }}}

	// {{{ processPush()
	/** 
	 * Processes an incoming LegiScan API JSON payload into an associative array
	 *
	 * @throws APIException
	 *
	 * @return boolean
	 *   Success of payload processing
	 *
	 */
	public function processPush()
	{
		// Check to see if the incoming payload is raw or cooked
		if ($_SERVER['CONTENT_TYPE'] != 'application/x-www-form-urlencoded') 
		{
			// Raw response waiting on STDIN
			$payload = file_get_contents('php://input');
		}
		else
		{
			// Cooked into a POST variable
			if (isset($_POST[$this->form_var]))
				$payload = $_POST[$this->form_var];
			else
				throw new APIException('processPush expected POST variable [' . $this->form_var . '] is not set');
		}

		// Decode payload into associative array instead of object
		$this->payload = json_decode($payload, true);
		if ($this->payload === false)
			throw new APIException('processPush could not decode API payload');

		// Look for root markers in payload to set type
		if (isset($this->payload['bill']))
			$this->payload_type = 'bill';
		elseif (isset($this->payload['text']))
			$this->payload_type = 'text';
		elseif (isset($this->payload['roll_call']))
			$this->payload_type = 'roll_call';
		elseif (isset($this->payload['person']))
			$this->payload_type = 'person';
		elseif (isset($this->payload['amendment']))
			$this->payload_type = 'amendment';
		elseif (isset($this->payload['supplement']))
			$this->payload_type = 'supplement';
		elseif (isset($this->payload['session']))
			$this->payload_type = 'session';
		else
			throw new APIException('processPush could not determine payload type [' . array_keys($this->payload)[0] . ']');

		return true;
	}
	// }}}

	// {{{ getPayload()
	/** 
	 * Returns the decoded payload associative array for the most recently processed
	 * record
	 *
	 * @return mixed[] The decoded payload array
	 *
	 */
	public function getPayload()
	{
		return $this->payload;
	}
	// }}}

	// {{{ getPayloadType()
	/** 
	 * Returns the type of payload that was most recently processed
	 *
	 * @return string The type of payload `[bill, text, amendment, supplement, roll_call, person, session]`
	 *
	 */
	public function getPayloadType()
	{
		return $this->payload_type;
	}
	// }}}

	// {{{ respondOk()
	/** 
	 * Wrapper function around respond() that creates a valid OK response
	 *
	 * @param array $missing
	 *   Optional array listing missing objects to request in the response
	 *
	 */
	public function respondOk($missing = array())
	{
		$response = array('status'=>LegiScan::API_OK);
		if (is_array($missing) && !empty($missing))
			$response['missing'] = $missing;
		$this->respond($response);
	}
	// }}}

	// {{{ respondError()
	/** 
	 * Wrapper function around respond() that creates a valid ERROR response
	 *
	 * @param string $error_message
	 *   A descriptive user-defined error message to explain the error response
	 *
	 */
	public function respondError($error_message)
	{
		$response = array(
			'status' => LegiScan::API_ERROR,
			'alert' => array(
				'message'=>$error_message
			),
		);
		$this->respond($response);
	}
	// }}}

	// {{{ respond()
	/** 
	 * Encode and send a valid API response back to LegiScan
	 *
	 * @param array $response
	 *   Contains the push endpoint server response structure
	 *
	 */
	private function respond($response)
	{
		header("Content-Type: application/json");

		$msg = json_encode($response);

		echo $msg;

		exit(0);
	}
	// }}}
}
// }}}

// {{{ LegiScan_Pull Class
/**
 * LegiScan API Pull Interface Class
 *
 * Basic pull class to generate API requests and decode the JSON responses. Doing
 * something interesting with the results is the job of {@link LegiScan_Process}.
 * Each `get*()` function maps to the corresponding API operation, preparing the
 * request array and processing through {@link LegiScan_Pull::apiRequest()}.
 *
 * @package LegiScan\API
 * @see LegiScan_CommandLine
 * @see LegiScan_Process
 * @see LegiScan_Worker
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/legiscan
 *
 */
class LegiScan_Pull
{
	// {{{ Class Variables

	/**
	 * LegiScan API key for requests from `config.php`
	 *
	 * @var string Assigned 32 character API hash
	 * @access protected
	 */
	protected $api_key;

	/**
	 * Raw response from an API call
	 *
	 * @var string
	 * @access protected
	 */
	protected $response;

	/**
	 * Decoded associative array representing the API response
	 *
	 * @var mixed[]
	 * @access protected
	 */
	protected $payload;

	/**
	 * Cache layer interface
	 *
	 * @var LegiScan_Cache_File
	 * @access protected
	 */
	protected $cache;

	/**
	 * Most recent Pull API request URL
	 *
	 * @var string
	 * @access protected
	 */
	protected $request_url;

	// }}}

	// {{{ __construct()
	/** 
	 * Class constructor that nominally validates the API key
	 *
	 * @throws APIException
	 *
	 * @param string $api_key
	 *   (**OPTIONAL**) Override the api_key from LegiScan::getConfig()
	 *
	 */
	function __construct()
	{
		$this->cache = new LegiScan_Cache_File('api');

		$this->api_key = LegiScan::getConfig('api_key');

		// Sanity checks to avoid sending junk requests
		if (!preg_match('/^[0-9a-f]{32}$/i', $this->api_key))
			throw new APIException('Invalid API key');
	}
	// }}}

	// {{{ getSessionList()
	/** 
	 * Get a session list for a specified state
	 *
	 * @param string $state
	 *   State to pull the session list for
	 * 
	 * @return array
	 *   The list of state sessions and their corresponding session_id numbers
	 */
	public function getSessionList($state)
	{
		$params = array('state'=>strtoupper($state));

		return $this->apiRequest('getSessionList', $params);
	}
	// }}}

	// {{{ getMasterList()
	/** 
	 * Get a master list of bills for a specific session_id
	 *
	 * @param integer $session_id
	 *   LegiScan session_id to pull the master list for
	 * 
	 * @return array
	 *   The master list of current bills with their corresponding bill_id numbers
	 */
	public function getMasterList($session_id)
	{
		$params = array('id'=>$session_id);

		// Preferred method is using session_id, but support the 
		// state abbr shortcut call mostly for legiscan-cli.php
		if (preg_match('#^[A-Z]{2}#i', $session_id))
			$params = array('state'=>$session_id);

		return $this->apiRequest('getMasterList', $params);
	}
	// }}}

	// {{{ getMasterListRaw()
	/** 
	 * Get a master list of bills for a specific session_id optimized for
	 * `change_hash` detection
	 *
	 * @param integer $session_id
	 *   LegiScan session_id to pull the master list for
	 * 
	 * @return array
	 *   The master list of current bills with their corresponding bill_id numbers
	 */
	public function getMasterListRaw($session_id)
	{
		$params = array('id'=>$session_id);

		// Preferred method is using session_id, but support the 
		// state abbr shortcut call mostly for legiscan-cli.php
		if (preg_match('#^[A-Z]{2}#i', $session_id))
			$params = array('state'=>$session_id);

		return $this->apiRequest('getMasterListRaw', $params);
	}
	// }}}

	// {{{ getBill()
	/** 
	 * Get the bill detail record for a specified bill
	 *
	 * @param integer $bill_id
	 *   The internal bill id for the requested bill
	 * 
	 * @return array
	 *   The record containing a summary of the bill detail information
	 */
	public function getBill($bill_id)
	{
		$params = array('id'=>$bill_id);

		return $this->apiRequest('getBill', $params);
	}
	// }}}

	// {{{ getBillText()
	/** 
	 * Get a copy of the bill text for the specified document id
	 *
	 * (<b>Note</b>: doc is base64 encoded)
	 *
	 * @param integer $text_id
	 *   The internal bill id for the requested bill text
	 * 
	 * @return array
	 *   The bill text record with the actual text and meta type information
	 */
	public function getBillText($text_id)
	{
		$params = array('id'=>$text_id);

		if (LegiScan::getConfig('prefer_pdf'))
			$params['prefer'] = 'pdf';

		return $this->apiRequest('getBillText', $params);
	}
	// }}}

	// {{{ getAmendment()
	/** 
	 * Get a copy of the bill amendment for the specified amendment id
	 *
	 * (<b>Note</b>: doc is base64 encoded)
	 *
	 * @param integer $amendment_id
	 *   The internal amendment id for the requested bill amendment
	 * 
	 * @return array
	 *   The bill amendment record with the actual amendment and meta type information
	 */
	public function getAmendment($amendment_id)
	{
		$params = array('id'=>$amendment_id);

		if (LegiScan::getConfig('prefer_pdf'))
			$params['prefer'] = 'pdf';

		return $this->apiRequest('getAmendment', $params);
	}
	// }}}

	// {{{ getSupplement()
	/** 
	 * Get a copy of the bill supplement for the specified supplement id
	 *
	 * (<b>Note</b>: doc is base64 encoded)
	 *
	 * @param integer $supplement_id
	 *   The internal supplement id for the requested bill supplement
	 * 
	 * @return array
	 *   The bill supplement record with the actual supplement and meta type information
	 */
	public function getSupplement($supplement_id)
	{
		$params = array('id'=>$supplement_id);

		if (LegiScan::getConfig('prefer_pdf'))
			$params['prefer'] = 'pdf';

		return $this->apiRequest('getSupplement', $params);
	}
	// }}}

	// {{{ getRollCall()
	/** 
	 * Get the roll call record for a specified bill with vote summary and
	 * individual legislator vote information
	 *
	 * @param integer $roll_call_id
	 *   The internal LegiScan roll_call_id for the requested vote
	 * 
	 * @return array
	 *   A detailed roll call record with individual vote data
	 */
	public function getRollCall($roll_call_id)
	{
		$params = array('id'=>$roll_call_id);

		return $this->apiRequest('getRollCall', $params);
	}
	// }}}

	// {{{ getPerson()
	/** 
	 * Get a basic people record for the specified id
	 *
	 * @param integer $people_id
	 *   The internal LegiScan people_id to be retrieved {@link apiRequest()}
	 * 
	 * @return array
	 *   The basic sponsor information for the given people_id
	 */
	public function getPerson($people_id)
	{
		$params = array('id'=>$people_id);

		return $this->apiRequest('getPerson', $params);
	}
	// }}}

	// {{{ getSearch()
	/** 
	 * Performs a search against the LegiScan national legislative database
	 *
	 * @throws APIException
	 *
	 * @param array $search
	 *   An associative array with the search parameters: `state`, `bill`,
	 *   `query`, `year`
	 * 
	 * @return array
	 *   A page of search results for the given query parameters
	 */
	public function getSearch($search)
	{
		// Check and build the search query parameters
		$params = array();

		// Limit to a state (abbreviation) or ALL, defaults to all
		if (isset($search['state']))
			$params['state'] = $search['state'];
		// Exact bill number search
		if (isset($search['bill']))
			$params['bill'] = $search['bill'];
		// Full text search query string
		if (isset($search['query']))
			$params['query'] = $search['query'];
		// Search year: 1=all,2=current,3=recent,4=prior,specific year otherwise
		if (isset($search['year']))
			$params['year'] = $search['year'];
		// Page of results to fetch, defaults to page 1
		if (isset($search['page']))
			$params['page'] = $search['page'];
		// Use a raw search (2000 count page size but only bill_id, score and change_hash)
		if (isset($search['raw']))
			$params['raw'] = 1;

		// Make sure at least something useful is being searched for
		if (!isset($search['bill']) && !isset($search['query']))
			throw new APIException('Missing required search parameters');

		return $this->apiRequest('getSearch', $params);
	}
	// }}}

	// {{{ getSearchRaw()
	/** 
	 * Performs a raw search against the LegiScan national legislative database
	 *
	 * @throws APIException
	 *
	 * @param array $search
	 *   An associative array with the search parameters: `state`, `bill`,
	 *   `query`, `year`
	 * 
	 * @return array
	 *   A page of search results for the given query parameters
	 */
	public function getSearchRaw($search)
	{
		// Check and build the search query parameters
		$params = array();

		// Limit to a state (abbreviation) or ALL, defaults to all
		if (isset($search['state']))
			$params['state'] = $search['state'];
		// Exact bill number search
		if (isset($search['bill']))
			$params['bill'] = $search['bill'];
		// Full text search query string
		if (isset($search['query']))
			$params['query'] = $search['query'];
		// Search year: 1=all,2=current,3=recent,4=prior,specific year otherwise
		if (isset($search['year']))
			$params['year'] = $search['year'];
		// Page of results to fetch, defaults to page 1
		if (isset($search['page']))
			$params['page'] = $search['page'];

		// Make sure at least something useful is being searched for
		if (!isset($search['bill']) && !isset($search['query']))
			throw new APIException('Missing required search parameters');

		return $this->apiRequest('getSearchRaw', $params);
	}
	// }}}

	// {{{ getDatasetList()
	/** 
	 * Get a list of available datasets with optional filters
	 *
	 * @param array $filter
	 *   Optional state and year filters
	 * 
	 * @return array
	 *   The list of datasets available and their corresponding access keys
	 */
	public function getDatasetList($filter)
	{
		$params = array();
		if (isset($filter['state']))
			$params['state'] = $filter['state'];
		if (isset($filter['year']))
			$params['year'] = $filter['year'];

		return $this->apiRequest('getDatasetList', $params);
	}
	// }}}

	// {{{ getDataset()
	/** 
	 * Get a dataset archive payload for a specific session id
	 *
	 * @param integer $session_id
	 *   The internal LegiScan session_id to be retrieved
	 * 
	 * @param string $access_key
	 *   A valid access key provided in getDatasetList
	 * 
	 * @return array
	 *   The dataset payload with the actual zip file and meta information
	 */
	public function getDataset($session_id, $access_key)
	{
		$params = array('id'=>$session_id,'access_key'=>$access_key);

		return $this->apiRequest('getDataset', $params);
	}
	// }}}

	// {{{ getSessionPeople()
	/** 
	 * Retrieve a list of people records active in a specific session id
	 *
	 * @param integer $session_id
	 *   The internal LegiScan session_id to scan
	 * 
	 * @return array
	 *   Collection of {@link getPerson()} equivalent records of those active in session
	 */
	public function getSessionPeople($session_id)
	{
		$params = array('id'=>$session_id);

		return $this->apiRequest('getSessionPeople', $params);
	}
	// }}}

	// {{{ getSponsoredList()
	/** 
	 * Retrieve a list of bills sponsored by an individual legislator 
	 *
	 * @param integer $people
	 *   The internal LegiScan people_id to look for
	 * 
	 * @return array
	 *   Information on the list of sponsored bills
	 */
	public function getSponsoredList($people_id)
	{
		$params = array('id'=>$people_id);

		return $this->apiRequest('getSponsoredList', $params);
	}
	// }}}

	// {{{ getMonitorList()
	/** 
	 * Retrieve a list of bills monitored in the GAITS account associated with the
	 * API key
	 *
	 * @param array $filter
	 *   An associative array with the filter parameters: `record`
	 *
	 * @return array
	 *   Information on the list of monitored bills
	 */
	public function getMonitorList($filter)
	{
		$params = array();
		if (isset($filter['record']))
			$params['record'] = $filter['record'];
		else
			$params['recrod'] = 'current';

		return $this->apiRequest('getMonitorList', $params);
	}
	// }}}

	// {{{ getMonitorListRaw()
	/** 
	 * Retrieve a list of bills monitored in the associated GAITS account, optimized for
	 * `change_hash` detection
	 *
	 * @param array $filter
	 *   An associative array with the filter parameters: `record`
	 *
	 * @return array
	 *   Information on the list of monitored bills
	 */
	public function getMonitorListRaw($filter)
	{
		$params = array();
		if (isset($filter['record']))
			$params['record'] = $filter['record'];
		else
			$params['record'] = 'current';

		return $this->apiRequest('getMonitorListRaw', $params);
	}
	// }}}

	// {{{ setMonitor()
	/** 
	 * Set the monitoring status of one or more `bill_id` in the GAITS account
	 * monitoring or ignore list
	 *
	 * @param array $monitor 
	 *   An associative array with the monitor parameters:
	 *   `action` - action to take: monitor, ignore, remove
	 *   `list` - bill_id list to operate on
	 *   `stance` - position on bill: watch, support, oppose
	 *
	 * @return array
	 *   Information on the list of monitored bills
	 */
	public function setMonitor($monitor)
	{
		$params = array();

		if (isset($monitor['action']))
			$params['action'] = $monitor['action'];
		if (isset($monitor['stance']))
			$params['stance'] = $monitor['stance'];

		$params['list'] = implode(',', $monitor['list']);

		return $this->apiRequest('setMonitor', $params);
	}
	// }}}

	// {{{ apiRequest()
	/** 
	 * Makes the actual request to the LegiScan API server via cURL
	 *
	 * @throws APIException
	 *
	 * @param string $op
	 *   The API operation to actually perform
	 * 
	 * @param array $params
	 *   An associative array of the required parameters to perform the
	 *   needed API operation
	 * 
	 * @return mixed[]
	 *   An associative array representing the API response
	 */
	protected function apiRequest($op, $params)
	{
		// Merge in the base parameters
		$query = array_merge(array('key'=>$this->api_key,'op'=>$op), $params);

		$query_string = http_build_query($query);

		$this->request_url = 'https://api.legiscan.com/?' . $query_string;

		// Use cache time guidelines per API Operations p.7 API User Manual
		$cache_times = array(
			'getsessionlist'	=> 86400,	// Daily
			'getmasterlist'		=> 3600,	// 1 hour
			'getmasterlistraw'	=> 3600,	// 1 hour
			'getbill'			=> 10800,	// 3 hours
			'getbilltext'		=> 2592000,	// Static (30 days)
			'getamendment'		=> 2592000,	// Static (30 days)
			'getsupplement'		=> 2592000,	// Static (30 days)
			'getrollcall'		=> 2592000,	// Static (30 days)
			'getperson'			=> 604800,	// Weekly
			'getsearch'			=> 3600,	// 1 hour
			'getsearchraw'		=> 3600,	// 1 hour
			'search'			=> 3600,	// 1 hour
			'searchraw'			=> 3600,	// 1 hour
			'getdatasetlist'	=> 86400,	// Weekly (1 day)
			'getdataset'		=> 86400,	// Weekly (1 day)
			'getsessionpeople'	=> 86400,	// Weekly (1 day)
			'getsponsoredlist'	=> 86400,	// Daily
			'getmonitorlist'	=> 0,		// Live hook
			'getmonitorlistraw'	=> 0,		// Live hook
			'setmonitor'		=> 0,		// Live hook
		);

		// Default 1 hour cache and check for guideline
		$lifetime = 3600;
		$nop = strtolower($op);
		if (isset($cache_times[$nop]))
			$lifetime = $cache_times[$nop];

		// Always check local cache first to save an actual API call
		$cache_file = $this->getCacheFilename($op, $params);
		$this->response = $this->cache->get($cache_file, $lifetime);

		// Flag if the cache needs updating
		$update_cache = false;

		if (!$this->response)
		{
			// Initialize curl and make the real request
			$ch = curl_init();
			curl_setopt($ch, CURLOPT_URL, $this->request_url);
			curl_setopt($ch, CURLOPT_FAILONERROR, true);
			curl_setopt($ch, CURLOPT_TIMEOUT, 30);
			curl_setopt($ch, CURLOPT_BUFFERSIZE, 64000);
			curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
			curl_setopt($ch, CURLOPT_USERAGENT, "LegiScan API Client " . LegiScan::VERSION);

			$this->response = curl_exec($ch);

			// See if something drastic happened
			if ($this->response === false)
				throw new APIException('Could not get response from LegiScan API server (cURL ' . curl_errno($ch) . ': ' . curl_error($ch) . ')');

			// Fresh data so update cache
			$update_cache = true;	
		}

		// Decode the response and check for error status
		$this->payload = json_decode($this->response, true);
		if ($this->payload === false)
			throw new APIException('Could not decode JSON API response');

		// If the API returned and error pitch it up the chain with an exception
		if ($this->payload['status'] == LegiScan::API_ERROR)
			throw new APIException($this->payload['alert']['message']);

		// Save to cache if needed and we get here without errors
		if ($update_cache)
			$this->cache->set($cache_file, $this->response);

		return $this->payload;
	}
	// }}}

	// {{{ importBillList()
	/**
	 * Process a bill_id list processing and requesting any necessary missing child objects
	 *
	 * @param array $payload
	 *   List of the LegiScan bill_ids that should be imported
	 *
	 * @param LegiScan_Process $logic
	 *   An instance of {@link LegiScan_Process} attached to its database
	 * 
	 * @return boolean
	 *   Indicating the success/failure of bill payload processing
	 */
	function importBillList($bills, $logic)
	{
		$n = count($bills) - 1;
		foreach ($bills as $i => $bill_id)
		{
			LegiScan::fileLog("importBillList requesting bill $bill_id ($i / $n)");

			$resp = $this->getBill($bill_id);
			if ($resp)
			{
				$missing = $logic->processBill($resp);

				if (!empty($missing))
				{
					if (isset($missing['texts']))
					{
						foreach ($missing['texts'] as $text_id)
						{
							LegiScan::fileLog("importBillList requesting text $text_id");
							$resp = $this->getBillText($text_id);
							if ($resp)
								$logic->processBillText($resp);
						}
					}
					if (isset($missing['amendments']))
					{
						foreach ($missing['amendments'] as $amendment_id)
						{
							LegiScan::fileLog("importBillList requesting amendment $amendment_id");
							$resp = $this->getAmendment($amendment_id);
							if ($resp)
								$logic->processAmendment($resp);
						}
					}
					if (isset($missing['supplements']))
					{
						foreach ($missing['supplements'] as $supplement_id)
						{
							LegiScan::fileLog("importBillList requesting supplement $supplement_id");
							$resp = $this->getSupplement($supplement_id);
							if ($resp)
								$logic->processSupplement($resp);
						}
					}
					if (isset($missing['sponsors']))
					{
						foreach ($missing['sponsors'] as $people_id)
						{
							LegiScan::fileLog("importBillList requesting person $people_id");
							$resp = $this->getPerson($people_id);
							if ($resp)
								$logic->processPerson($resp);
						}
					}
					if (isset($missing['votes']))
					{
						foreach ($missing['votes'] as $roll_call_id)
						{
							LegiScan::fileLog("importBillList requesting roll_call $roll_call_id");
							$resp = $this->getRollCall($roll_call_id);
							if ($resp)
							{
								$people_list = $logic->processRollCall($resp);
								foreach ($people_list as $people_id)
								{
									LegiScan::fileLog("importBillList requesting person $people_id");
									$resp = $this->getPerson($people_id);
									if ($resp)
										$logic->processPerson($resp);
								}
							}
						}
					}
				}
			}

			// May have hammered for missing items, plus just be nice =)
			if ($i != $n)
				sleep(1);
		}

		return true;
	}
	// }}}

	// {{{ getURL()
	/** 
	 * Return the most recent Pull API request URL generated by {@see apiRequest()}
	 *
	 * @return string
	 *   The full request URL
	 */
	function getURL()
	{
		return $this->request_url;
	}
	// }}}

	// {{{ getRawResponse()
	/** 
	 * Return the JSON payload of the most recent Pull API request by {@see apiRequest()}
	 *
	 * @return string
	 *   The JSON data structure
	 */
	function getRawResponse()
	{
		return $this->response;
	}
	// }}}

	// {{{ getCacheFilename()
	/**
	 * Generate an API cache filename 
	 *
	 * @param string $op
	 *   The API operation hook name
	 *
	 * @param array $params
	 *   The API paramaters that were part of the call
	 * 
	 * @return string
	 *   Filename path fragment for the cache location under cache root
	 */
	function getCacheFilename($op, $params)
	{
		$op = strtolower($op);

		switch ($op)
		{
			case 'getbill':
				$filename = 'bill/' . $params['id'] . '.json';
				break;
			case 'getperson':
				$filename = 'people/' . $params['id'] . '.json';
				break;
			case 'getrollcall':
				$filename = 'rollcall/' . $params['id'] . '.json';
				break;
			case 'getbilltext':
				$filename = 'text/' . $params['id'] . '.json';
				break;
			case 'getamendment':
				$filename = 'amendment/' . $params['id'] . '.json';
				break;
			case 'getsupplement':
				$filename = 'supplement/' . $params['id'] . '.json';
				break;
			case 'getsessionlist':
				$filename = 'sessionlist/' . $params['state'] . '.json';
				break;
			case 'getmasterlist':
			case 'getmasterlistraw':
				$raw = '';
				if (isset($params['raw']) || $op == 'getmasterlistraw')
					$raw = '_raw';
				// If state is present prefer that
				if (isset($params['state']))
					$filename = 'masterlist/' . $params['state'] . $raw . '.json';
				else
					$filename = 'masterlist/' . $params['id'] . $raw . '.json';
				break;
			case 'search':
			case 'searchraw':
			case 'getsearch':
			case 'getsearchraw':
				$chunks = array();
				if (isset($params['state']))
					$chunks[] = $params['state'];
				if (isset($params['raw']) || $op == 'searchraw')
					$chunks[] = 'raw';
				if (isset($params['bill']))
					$chunks[] = $params['bill'];
				if (isset($params['year']))
					$chunks[] = 'y' . $params['year'];
				if (isset($params['page']))
					$chunks[] = 'p' . $params['page'];
				if (isset($params['query']))
					$chunks[] = $params['query'];

				$file_chunk = strtolower(implode('_', $chunks));

				// NOTE: To maintain some sort of readability to the file name
				// do a quick transformation.
				$file_chunk = mb_ereg_replace("([^\w\s\d\-_~,;\[\]\(\).])", '', $file_chunk);
				$file_chunk = mb_ereg_replace("([\.]{2,})", '', $file_chunk);
				// Very long searches will break max filename length, check and adjust
				// accounting for extension
				if ((strlen($file_chunk) + 5) > 255)
					$file_chunk = substr($file_chunk, 0, 250);

				$filename = 'search/' . $file_chunk . '.json';
				break;
			case 'getdatasetlist':
				$chunks = array();
				$chunks[] = 'list';
				if (isset($params['state']))
					$chunks[] = $params['state'];
				if (isset($params['year']))
					$chunks[] = 'y' . $params['year'];

				$file_chunk = strtolower(implode('_', $chunks));

				$filename = 'datasetlist/' . $file_chunk . '.json';
				break;
			case 'getdataset':
				$filename = 'dataset/' . $params['id'] . '.json';
				break;
			case 'getsessionpeople':
				$filename = 'sessionpeople/' . $params['id'] . '.json';
				break;
			case 'getsponsoredlist':
				$filename = 'sponsoredlist/' . $params['id'] . '.json';
				break;
			case 'getmonitorlist':
			case 'getmonitorlistraw':
				$raw = '';
				if ($op == 'getmonitorlistraw')
					$raw = '_raw';
				$filename = 'monitorlist/' . $params['record'] . $raw . '.json';
				break;
			case 'setmonitor':
				$filename = 'monitorlist/' . $params['action'] . '.json';
				break;
			default:
				throw new APIException("Cannot determine Pull API cache file for $op");
				break;
		}

		return $filename;
	}
	// }}}
}
// }}}

// {{{ LegiScan_Bulk Class
/**
 * LegiScan API Bulk Dataset Class
 *
 * Worker class that processes a LegiScan dataset archive and imports / updates
 * the local database, passing the individual JSON payloads to their corresponding
 * handlers in {@see LegiScan_Process}. Used by {@see LegiScan_Import} in the
 * `legiscan-bulk.php` utility.
 *
 * @package LegiScan\API
 * @see LegiScan_Import
 * @see LegiScan_Process
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/datasets
 *
 */
class LegiScan_Bulk
{
	// {{{ Class Variables
	/**
	 * Instance of {@see LegiScan_Process}
	 *
	 * @var LegiScan_Process
	 */	
	protected $logic;
	// }}}

	// {{{ __construct()
	/** 
	 * Class constructor, initilizes necessary {@see LegiScan_Process} instances
	 *
	 */
	function __construct()
	{
		$this->logic = new LegiScan_Process();
	}
	// }}}

	// {{{ importDataset()
	/**
	 * Process LegiScan session dataset ZIP archive and import into database {@see LegiScan_Import}
	 *
	 * @param string $zipfile
	 *   Path to a zip archive from LegiScan datasets or subscriber snapshot
	 *
	 * @param mixed[] $params
	 *   Additional parameters to control behavior and output
	 *
	 * @return mixed[]
	 *   Array with hash and counts for the number of objects imported
	 */
	function importDataset($zipfile, $params)
	{
		$dry_run = isset($params['dry_run']) ? $params['dry_run'] : false;
		$verbose = isset($params['verbose']) ? $params['verbose'] : false;
		$debug = isset($params['debug']) ? $params['debug'] : false;
		$expected_hash = isset($params['expected_hash']) ? $params['expected_hash'] : '';

		$hash = '';
		$import_stats = array();

		$timer_start = microtime(true);

		try {
			$zip = new ZipArchive();

			// Everybody likes numbers
			$import_stats = array(
				'bill' => 0,
				'people' => 0,
				'vote' => 0,
				'text' => 0,
				'amendment' => 0,
				'supplement' => 0,
			);

			$zip->open($zipfile);

			// Last file is always hash.md5
			$import_hash = $zip->getFromIndex($zip->numFiles - 1);
			$import_date = strftime('%Y-%m-%d', $zip->statIndex($zip->numFiles - 1)['mtime']);
			$import_session_id = 0;

			// If there is an expected hash, check it
			if ($expected_hash && $import_hash != $expected_hash)
			{
				throw new APIException("processDataset hash mismatch");
			}

			for ($i = 0; $i < $zip->numFiles; $i++)
			{
				$name = $zip->statIndex($i)['name'];
				$basename = basename($name);

				if (stripos($basename, '.json') !== false)
				{
					// Decode the payload into an associative array
					$payload = json_decode($zip->getFromIndex($i), true);

					if ($payload)
					{
						// Use path information to determine payload type

						if (stripos($name, '/bill/') !== false)
						{
							if ($debug) echo "\tbill {$payload['bill']['bill_id']} from $basename\n";
							if (!$dry_run) $this->logic->processBill($payload);
							$import_stats['bill']++;
							if (!$import_session_id)
								$import_session_id = $payload['bill']['session_id'];
						}
						elseif (stripos($name, '/people/') !== false)
						{
							if ($debug) echo "\tperson {$payload['person']['people_id']} from $basename\n";
							if (!$dry_run) $this->logic->processPerson($payload);
							$import_stats['people']++;
						}
						elseif (stripos($name, '/vote/') !== false)
						{
							if ($debug) echo "\troll_call {$payload['roll_call']['roll_call_id']} from $basename\n";
							if (!$dry_run) $this->logic->processRollCall($payload);
							$import_stats['vote']++;
						}
						elseif (stripos($name, '/text/') !== false)
						{
							if ($debug) echo "\ttext {$payload['text']['doc_id']} from $basename\n";
							if (!$dry_run) $this->logic->processBillText($payload);
							$import_stats['text']++;
						}
						elseif (stripos($name, '/amendment/') !== false)
						{
							if ($debug) echo "\tamendment {$payload['amendment']['amendment_id']} from $basename\n";
							if (!$dry_run) $this->logic->processAmendment($payload);
							$import_stats['amendment']++;
						}
						elseif (stripos($name, '/supplement/') !== false)
						{
							if ($debug) echo "\tsupplement {$payload['supplement']['supplement_id']} from $basename\n";
							if (!$dry_run) $this->logic->processSupplement($payload);
							$import_stats['supplement']++;
						}
					}
					else
					{
						throw new APIException("import could not decode API payload $name");
					}
				}
			}

			$zip->close();

			$timer_end = microtime(true);
			$time_elapsed = $timer_end - $timer_start;
			$time = sec2hms(round($time_elapsed));

			$db = $this->logic->getDB();
			$sql = 'UPDATE ls_session SET import_date = :import_date, import_hash = :import_hash WHERE session_id = :session_id';
			$stmt = $db->prepare($sql);
			$stmt->bindValue(':session_id', $import_session_id, PDO::PARAM_INT);
			$stmt->bindValue(':import_hash', $import_hash, PDO::PARAM_STR);
			$stmt->bindValue(':import_date', $import_date, PDO::PARAM_STR);
			$stmt->execute();

			$stats = array();
			$stats[] = '[' . $import_session_id . ']';
			$stats[] = basename($zipfile);
			foreach ($import_stats as $k => $v)
			{
				if ($v)
				{
					if ($verbose) echo sprintf("%10d %s\n", $v, $k);
					$stats[] = $v . ' ' . $k;
				}
			}
			if ($verbose) echo sprintf("%10s elapsed time\n", $time);
			$stats[] = 'in ' . $time;

			$log_msg = "LegiScan Import Complete: " . implode(' ', $stats);
			LegiScan::fileLog($log_msg);

		} catch (Exception $e) {
			// Catch any other errors and push back an API error message
			$error_msg = "LegiScan Import ERROR: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";
		}

		$response = array(
			'hash' => $import_hash,
			'stats' => $import_stats,
		);

		return $response;
	}
	// }}}
}
// }}}

// {{{ LegiScan_Process Class
/**
 * LegiScan API Database Process Class
 *
 * Class that processes decoded API payloads from {@link LegiScan_Bulk},
 * {@link LegiScan_Pull} or {@link LegiScan_Push} into the database. Each
 * `process*()` function maps to the corresponding API response to INSERT
 * or UPDATE the necessary data.
 *
 * @package LegiScan\API
 * @see LegiScan_CommandLine
 * @see LegiScan_Import
 * @see LegiScan_Endpoint
 * @see LegiScan_Worker
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/legiscan
 *
 */
class LegiScan_Process
{

	// {{{ Class Variables
	/**
	 * Array of object ids that are missing and should be requested
	 *
	 * @var array[]
	 */
	protected $missing;

	/**
	 * PDO Database object handle
	 *
	 * @var PDO
	 */
	protected $db;

	/**
	 * LegiScan document cache object handle
	 *
	 * @var LegiScan_Cache_File
	 */
	protected $cache;

	/**
	 * LegiScan memory cache object handle
	 *
	 * @var LegiScan_Cache_Memory
	 */
	protected $memcache;

	/**
	 * Should dates be massaged for PostgreSQL in case of '0000-00-00'
	 *
	 * @var boolean
	 */
	private $massage;
	// }}}

	// {{{ __construct()
	/** 
	 * Class constructor, connects to database and caching layers 
	 *
	 * @throws PDOException
	 *
	 */
	function __construct()
	{
		// Setup the document cache handle
		$this->cache = new LegiScan_Cache_File('doc');

		// Grab the memory cache instance
		$this->memcache = LegiScan_Cache_Memory::getInstance();

		// Initialize the missing array
		$this->resetMissing();

		$dsn = LegiScan::getConfig('dsn');
		$username = LegiScan::getConfig('db_user');
		$password = LegiScan::getConfig('db_pass');
		$pdo_options = array(
			PDO::ATTR_ERRMODE				=> PDO::ERRMODE_EXCEPTION,
			PDO::ATTR_DEFAULT_FETCH_MODE	=> PDO::FETCH_ASSOC,
			PDO::ATTR_EMULATE_PREPARES		=> false,
		);

		// For MySQL force add charset to dsn, preferred over SET NAMES
		if (stripos($dsn, 'mysql') === 0 && stripos($dsn, 'charset=utf8') === false)
			$dsn .= ';charset=utf8';

		$this->massage = (bool) LegiScan::getConfig('massage_dates', false);
		
		// PostgreSQL will need the '0000-00-00' date massaged to NULL
		if (stripos($dsn, 'pgsql') !== false)
			$this->massage = true;

		// For MSSQL force the issue
		if (stripos($dsn, 'sqlsrv') !== false)
			$this->massage = true;

		$this->db = new PDO($dsn, $username, $password, $pdo_options);
	}
	// }}}

	// {{{ processSessionList()
	/** 
	 * Process a session list payload
	 *
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a session list
	 * 
	 * @return boolean
	 *   Indicating the success/failure of session list payload processing
	 */
	public function processSessionList($payload)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processSessionList payload status = "' . $payload['status'] . '"');

		$sessions = $payload['sessions'];

		try {
			foreach ($sessions as $session)
			{
				if ($this->checkExists('session', $session['session_id']))
					$sql = "UPDATE ls_session
							SET state_id = :state_id, year_start = :year_start, year_end = :year_end,
								prefile = :prefile, prior = :prior, sine_die = :sine_die,
								special = :special, session_title = :session_title,
								session_name = :session_name, session_tag = :session_tag
							WHERE session_id = :session_id";
				else
					$sql = "INSERT INTO ls_session (
								session_id, state_id, year_start, year_end, prefile, prior, special,
								sine_id, session_title, session_name, session_tag
							) VALUES (
								:session_id, :state_id, :year_start, :year_end, :prefile, :prior,
								:sine_die, :special, :session_title, :session_name, :session_tag
							)";

				$this->db->beginTransaction();

				$stmt = $this->db->prepare($sql);
				$stmt->bindValue(':session_id', $session['session_id'], PDO::PARAM_INT);
				$stmt->bindValue(':state_id', $session['state_id'], PDO::PARAM_INT);
				$stmt->bindValue(':year_start', $session['year_start'], PDO::PARAM_INT);
				$stmt->bindValue(':year_end', $session['year_end'], PDO::PARAM_INT);
				$stmt->bindValue(':special', $session['special'], PDO::PARAM_INT);
				$stmt->bindValue(':prefile', $session['prefile'], PDO::PARAM_INT);
				$stmt->bindValue(':sine_die', $session['sine_die'], PDO::PARAM_INT);
				$stmt->bindValue(':prior', $session['prior'], PDO::PARAM_INT);
				$stmt->bindValue(':session_title', $session['session_title'], PDO::PARAM_STR);
				$stmt->bindValue(':session_name', $session['session_name'], PDO::PARAM_STR);
				$stmt->bindValue(':session_tag', $session['session_name'], PDO::PARAM_STR);
				$stmt->execute();

				$this->db->commit();

			}
		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processSession()
	/** 
	 * Process a single session payload
	 *
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a session
	 * 
	 * @return boolean
	 *   Indicating the success/failure of session payload processing
	 *
	 */
	public function processSession($payload)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processSession payload status = "' . $payload['status'] . '"');

		$session = $payload['session'];

		try {
			if ($this->checkExists('session', $session['session_id']))
				$sql = "UPDATE ls_session
						SET state_id = :state_id, year_start = :year_start, year_end = :year_end,
							prefile = :prefile, sine_die = :sine_die, prior = :prior,
							special = :special, session_title = :session_title,
							session_name = :session_name, session_tag = :session_tag
						WHERE session_id = :session_id";
			else
				$sql = "INSERT INTO ls_session (
							session_id, state_id, year_start, year_end, prefile, sine_die, prior,
							special, session_title, session_name, session_tag
						) VALUES (
							:session_id, :state_id, :year_start, :year_end, :prefile, :sine_die, :prior,
							:special, :session_title, :session_name, :session_tag
						)";

			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':session_id', $session['session_id'], PDO::PARAM_INT);
			$stmt->bindValue(':state_id', $session['state_id'], PDO::PARAM_INT);
			$stmt->bindValue(':year_start', $session['year_start'], PDO::PARAM_INT);
			$stmt->bindValue(':year_end', $session['year_end'], PDO::PARAM_INT);
			$stmt->bindValue(':prefile', $session['prefile'], PDO::PARAM_INT);
			$stmt->bindValue(':prior', $session['prior'], PDO::PARAM_INT);
			$stmt->bindValue(':sine_die', $session['sine_die'], PDO::PARAM_INT);
			$stmt->bindValue(':special', $session['special'], PDO::PARAM_INT);
			$stmt->bindValue(':session_title', $session['session_title'], PDO::PARAM_STR);
			$stmt->bindValue(':session_name', $session['session_name'], PDO::PARAM_STR);
			$stmt->bindValue(':session_tag', $session['session_tag'], PDO::PARAM_STR);
			$stmt->execute();

			$this->db->commit();

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processMasterList()
	/** 
	 * Process a master list payload, adding any necessary missing child objects to 
	 * the missing list
	 *
	 * @throws APIStatusException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a master list
	 *
	 * @param integer $import_type 
	 *   The `LegiScan::IMPORT_*` to control how bills are selected for import
	 * 
	 * @return integer[]
	 *   Returns an array of missing/changed bill_id or FALSE on processing errors
	 */
	public function processMasterList($payload, $import_type = LegiScan::IMPORT_ALL)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processMasterList payload status = "' . $payload['status'] . '"');

		$session = array_shift($payload['masterlist']);

		foreach ($payload['masterlist'] as $bill)
		{
			switch ($import_type)
			{
				case LegiScan::IMPORT_NEW:
					if (!$this->checkExists('bill', $bill['bill_id']))
						$this->request('bills', $bill['bill_id']);
					break;

				case LegiScan::IMPORT_CHANGED:
				case LegiScan::IMPORT_ALL:
					$sql = "SELECT bill_id
							FROM ls_bill
							WHERE bill_id = :bill_id AND change_hash = :change_hash";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':change_hash', $bill['change_hash'], PDO::PARAM_STR);
					$stmt->execute();
					$exists = $stmt->fetchColumn();

					if (!$exists)
						$this->request('bills', $bill['bill_id']);
					break;
			}
		}

		if (isset($this->missing['bills']))
			return $this->missing['bills'];
		else
			return array();
	}
	// }}}

	// {{{ processBill()
	/**
	 * Process a bill payload, adding any necessary missing child objects to 
	 * the missing list
	 *
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a bill
	 * 
	 * @return mixed[]
	 *   Array of objects : ids that are missing and need to be requested
	 *
	 */
	public function processBill($payload)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processBill payload status = "' . $payload['status'] . '"');

		$bill = $payload['bill'];
		$bill_id = $bill['bill_id'];

		$now = date('Y-m-d H:i:s');

		try {
			$exists_id = $this->checkExists('bill', $bill['bill_id']);

			$this->db->beginTransaction();

			if (!$exists_id)
			{
				// New bill inserts are easy...

				// {{{ New Bill

				// This could use makeSQLStatement like the existing bill path,
				// instead stay verbose for clarity to familiarize users with
				// structure

				// {{{ Session
				if (!$this->checkExists('session', $bill['session_id']))
				{
					$sql = "INSERT INTO ls_session (
								session_id, state_id, year_start, year_end, prefile, sine_die, prior,
								special, session_name, session_title, session_tag
							) VALUES (
								:session_id, :state_id, :year_start, :year_end, :prefile, :sine_die, :prior,
								:special, :session_name, :session_title, :session_tag
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':session_id', $bill['session_id'], PDO::PARAM_INT);
					$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
					$stmt->bindValue(':year_start', $bill['session']['year_start'], PDO::PARAM_INT);
					$stmt->bindValue(':year_end', $bill['session']['year_end'], PDO::PARAM_INT);
					$stmt->bindValue(':special', $bill['session']['special'], PDO::PARAM_INT);
					$stmt->bindValue(':session_name', $bill['session']['session_name'], PDO::PARAM_STR);
					$stmt->bindValue(':session_title', $bill['session']['session_title'], PDO::PARAM_STR);
					$stmt->bindValue(':session_tag', $bill['session']['session_tag'], PDO::PARAM_STR);
					$stmt->bindValue(':prefile', $bill['session']['prefile'], PDO::PARAM_INT);
					$stmt->bindValue(':sine_die', $bill['session']['sine_die'], PDO::PARAM_INT);
					$stmt->bindValue(':prior', $bill['session']['prior'], PDO::PARAM_INT);
					$stmt->execute();
					
					$this->middlewareSignal('session', $bill['session_id']);
				}
				// }}}

				// {{{ Base Bill
				$sql = "INSERT INTO ls_bill (
							bill_id, state_id, session_id, body_id, current_body_id,
							bill_type_id, bill_number, pending_committee_id,
							status_id, status_date, title, description,
							legiscan_url, state_url, change_hash,
							updated, created
						) VALUES (
							:bill_id, :state_id, :session_id, :body_id, :current_body_id,
							:bill_type_id, :bill_number, :pending_committee_id,
							:status_id, :status_date, :title, :description,
							:legiscan_url, :state_url, :change_hash,
							:updated, :created
						)";

				$stmt = $this->db->prepare($sql);
				$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
				$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
				$stmt->bindValue(':session_id', $bill['session']['session_id'], PDO::PARAM_INT);
				$stmt->bindValue(':body_id', $bill['body_id'], PDO::PARAM_INT);
				$stmt->bindValue(':current_body_id', $bill['current_body_id'], PDO::PARAM_INT);
				$stmt->bindValue(':bill_type_id', $bill['bill_type_id'], PDO::PARAM_INT);
				$stmt->bindValue(':bill_number', $bill['bill_number'], PDO::PARAM_STR);
				$stmt->bindValue(':pending_committee_id', $bill['pending_committee_id'], PDO::PARAM_INT);
				$stmt->bindValue(':status_id', $bill['status'], PDO::PARAM_INT);
				$stmt->bindValue(':status_date', $this->dbDate($bill['status_date']), PDO::PARAM_STR);
				$stmt->bindValue(':title', $bill['title'], PDO::PARAM_STR);
				$stmt->bindValue(':description', $bill['description'], PDO::PARAM_STR);
				$stmt->bindValue(':legiscan_url', $bill['url'], PDO::PARAM_STR);
				$stmt->bindValue(':state_url', $bill['state_link'], PDO::PARAM_STR);
				$stmt->bindValue(':change_hash', $bill['change_hash'], PDO::PARAM_STR);
				$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);
				$stmt->execute();
				// }}}

				// {{{ New Committee Check
				if ($bill['pending_committee_id'] && !$this->checkExists('committee', $bill['pending_committee_id']))
				{
					$sql = "INSERT INTO ls_committee (
								committee_id, committee_body_id, committee_name
							) VALUES (
								:committee_id, :committee_body_id, :committee_name
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':committee_id', $bill['committee']['committee_id'], PDO::PARAM_INT);
					$stmt->bindValue(':committee_body_id', $bill['committee']['chamber_id'], PDO::PARAM_INT);
					$stmt->bindValue(':committee_name', $bill['committee']['name'], PDO::PARAM_STR);
					$stmt->execute();
				}
				// }}}

				// {{{ Referral Chain
				if (!empty($bill['referrals']))
				{
					foreach ($bill['referrals'] as $idx => $link)
					{
						if (!$this->checkExists('committee', $link['committee_id']))
						{
							$sql = "INSERT INTO ls_committee (
								committee_id, committee_body_id, committee_name
							) VALUES (
								:committee_id, :committee_body_id, :committee_name
							)";

							$stmt = $this->db->prepare($sql);
							$stmt->bindValue(':committee_id', $link['committee_id'], PDO::PARAM_INT);
							$stmt->bindValue(':committee_body_id', $link['chamber_id'], PDO::PARAM_INT);
							$stmt->bindValue(':committee_name', $link['name'], PDO::PARAM_STR);
							$stmt->execute();
						}

						$step = $idx + 1; // Add 1 to 0 based index

						$sql = "INSERT INTO ls_bill_referral (
							bill_id, referral_step, referral_date, committee_id
						) VALUES (
							:bill_id, :referral_step, :referral_date, :committee_id
						)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':referral_step', $step, PDO::PARAM_INT);
						$stmt->bindValue(':referral_date', $this->dbDate($link['date']), PDO::PARAM_STR);
						$stmt->bindValue(':committee_id', $link['committee_id'], PDO::PARAM_INT);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Reasons
				if (isset($bill['reasons']))
				{
					foreach ($bill['reasons'] as $reason_id => $reason)
					{
						$sql = "INSERT INTO ls_bill_reason (
									bill_id, reason_id, created
								) VALUES (
									:bill_id, :reason_id, :created
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':reason_id', $reason_id, PDO::PARAM_INT);
						$stmt->bindValue(':created', $now, PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Progress
				foreach ($bill['progress'] as $step => $progress)
				{
					$step += 1; // Add 1 to 0 based index

					$sql = "INSERT INTO ls_bill_progress (
								bill_id, progress_step, progress_date,
								progress_event_id
							) VALUES (
								:bill_id, :progress_step, :progress_date,
								:progress_event_id
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':progress_step', $step, PDO::PARAM_INT);
					$stmt->bindValue(':progress_date', $this->dbDate($progress['date']), PDO::PARAM_STR);
					$stmt->bindValue(':progress_event_id', $progress['event'], PDO::PARAM_INT);
					$stmt->execute();
				}
				// }}}

				// {{{ History
				foreach ($bill['history'] as $step => $history)
				{
					$step += 1; // Add 1 to 0 based index

					$sql = "INSERT INTO ls_bill_history (
								bill_id, history_step, history_major,
								history_body_id, history_date, history_action
							) VALUES (
								:bill_id, :history_step, :history_major,
								:history_body_id, :history_date, :history_action
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':history_step', $step, PDO::PARAM_INT);
					$stmt->bindValue(':history_major', $history['importance'], PDO::PARAM_INT);
					$stmt->bindValue(':history_body_id', $history['chamber_id'], PDO::PARAM_INT);
					$stmt->bindValue(':history_date', $this->dbDate($history['date']), PDO::PARAM_STR);
					$stmt->bindValue(':history_action', $history['action'], PDO::PARAM_STR);
					$stmt->execute();
				}
				// }}}

				// {{{ Sponsors
				foreach ($bill['sponsors'] as $person)
				{
					$sql = "INSERT INTO ls_bill_sponsor (
								bill_id, people_id, sponsor_order, sponsor_type_id
							) VALUES (
								:bill_id, :people_id, :sponsor_order, :sponsor_type_id
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':people_id', $person['people_id'], PDO::PARAM_INT);
					$stmt->bindValue(':sponsor_order', $person['sponsor_order'], PDO::PARAM_INT);
					$stmt->bindValue(':sponsor_type_id', $person['sponsor_type_id'], PDO::PARAM_INT);
					$stmt->execute();

					if (($people_exists_id = $this->checkExists('people', $person['people_id'])))
						$sql = "UPDATE ls_people
								SET state_id = :state_id, role_id = :role_id, party_id = :party_id,
									name = :name, first_name = :first_name, middle_name = :middle_name,
									last_name = :last_name, suffix = :suffix, nickname = :nickname,
									district = :district, committee_sponsor_id = :committee_id,
									votesmart_id = :votesmart_id, followthemoney_eid = :followthemoney_eid,
									opensecrets_id = :opensecrets_id, ballotpedia = :ballotpedia,
									knowwho_pid = :knowwho_pid, person_hash = :person_hash,
									updated = :updated
								WHERE people_id = :people_id";
					else
						$sql = "INSERT INTO ls_people (
									people_id, state_id, role_id, party_id, name, first_name,
									middle_name, last_name, suffix, nickname, district,
									committee_sponsor_id, votesmart_id, followthemoney_eid,
									opensecrets_id, ballotpedia, knowwho_pid, person_hash,
									updated, created
								) VALUES (
									:people_id, :state_id, :role_id, :party_id, :name, :first_name,
									:middle_name, :last_name, :suffix, :nickname, :district,
									:committee_id, :votesmart_id, :followthemoney_eid,
									:opensecrets_id, :ballotpedia, :knowwho_pid, :person_hash,
									:updated, :created
								)";

					if ($people_exists_id)
						$p = $this->db->query("SELECT person_hash FROM ls_people WHERE people_id = {$people_exists_id}")->fetch();

					if (!$people_exists_id || $p['person_hash'] != $person['person_hash'])
					{
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':people_id', $person['people_id'], PDO::PARAM_INT);
						$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
						$stmt->bindValue(':role_id', $person['role_id'], PDO::PARAM_INT);
						$stmt->bindValue(':party_id', $person['party_id'], PDO::PARAM_INT);
						$stmt->bindValue(':name', $person['name'], PDO::PARAM_STR);
						$stmt->bindValue(':first_name', $person['first_name'], PDO::PARAM_STR);
						$stmt->bindValue(':middle_name', $person['middle_name'], PDO::PARAM_STR);
						$stmt->bindValue(':last_name', $person['last_name'], PDO::PARAM_STR);
						$stmt->bindValue(':suffix', $person['suffix'], PDO::PARAM_STR);
						$stmt->bindValue(':nickname', $person['nickname'], PDO::PARAM_STR);
						$stmt->bindValue(':district', $person['district'], PDO::PARAM_STR);
						$stmt->bindValue(':committee_id', $person['committee_id'], PDO::PARAM_INT);
						$stmt->bindValue(':ballotpedia', $person['ballotpedia'], PDO::PARAM_STR);
						$stmt->bindValue(':followthemoney_eid', $person['ftm_eid'], PDO::PARAM_INT);
						$stmt->bindValue(':votesmart_id', $person['votesmart_id'], PDO::PARAM_INT);
						$stmt->bindValue(':knowwho_pid', $person['knowwho_pid'], PDO::PARAM_INT);
						$stmt->bindValue(':opensecrets_id', $person['opensecrets_id'], PDO::PARAM_STR);
						$stmt->bindValue(':person_hash', $person['person_hash'], PDO::PARAM_STR);
						$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
						if (!$people_exists_id)
							$stmt->bindValue(':created', $now, PDO::PARAM_STR);

						$stmt->execute();
					}
				}
				// }}}

				// {{{ Votes
				foreach ($bill['votes'] as $vote)
				{
					$sql = "INSERT INTO ls_bill_vote (
								roll_call_id, bill_id, roll_call_body_id, roll_call_date,
								roll_call_desc, yea, nay, nv, absent, total, passed,
								legiscan_url, state_url, updated, created
							) VALUES (
								:roll_call_id, :bill_id, :roll_call_body_id, :roll_call_date,
								:roll_call_desc, :yea, :nay, :nv, :absent, :total, :passed,
								:legiscan_url, :state_url, :updated, :created
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':roll_call_id', $vote['roll_call_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':roll_call_body_id', $vote['chamber_id'], PDO::PARAM_INT);
					$stmt->bindValue(':roll_call_date', $this->dbDate($vote['date']), PDO::PARAM_STR);
					$stmt->bindValue(':roll_call_desc', $vote['desc'], PDO::PARAM_STR);
					$stmt->bindValue(':yea', $vote['yea'], PDO::PARAM_INT);
					$stmt->bindValue(':nay', $vote['nay'], PDO::PARAM_INT);
					$stmt->bindValue(':nv', $vote['nv'], PDO::PARAM_INT);
					$stmt->bindValue(':absent', $vote['absent'], PDO::PARAM_INT);
					$stmt->bindValue(':total', $vote['total'], PDO::PARAM_INT);
					$stmt->bindValue(':passed', $vote['passed'], PDO::PARAM_INT);
					$stmt->bindValue(':legiscan_url', $vote['url'], PDO::PARAM_STR);
					$stmt->bindValue(':state_url', $vote['state_link'], PDO::PARAM_STR);
					$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
					$stmt->bindValue(':created', $now, PDO::PARAM_STR);
					$stmt->execute();

					if (LegiScan::getConfig('want_vote_details'))
						$this->request('votes', $vote['roll_call_id']);
				}
				// }}}

				// {{{ Texts
				foreach ($bill['texts'] as $text)
				{
					$sql = "INSERT INTO ls_bill_text (
								text_id, bill_id, local_copy, bill_text_type_id,
								bill_text_mime_id, bill_text_date,
								legiscan_url, state_url, bill_text_size, bill_text_hash,
								updated, created
							) VALUES (
								:text_id, :bill_id, :local_copy, :bill_text_type_id,
								:bill_text_mime_id, :bill_text_date,
								:legiscan_url, :state_url, :bill_text_size, :bill_text_hash,
								:updated, :created
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':text_id', $text['doc_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
					$stmt->bindValue(':bill_text_size', $text['text_size'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_text_hash', $text['text_hash'], PDO::PARAM_STR);
					$stmt->bindValue(':bill_text_type_id', $text['type_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_text_mime_id', $text['mime_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_text_date', $this->dbDate($text['date']), PDO::PARAM_STR);
					$stmt->bindValue(':legiscan_url', $text['url'], PDO::PARAM_STR);
					$stmt->bindValue(':state_url', $text['state_link'], PDO::PARAM_STR);
					$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
					$stmt->bindValue(':created', $now, PDO::PARAM_STR);
					$stmt->execute();

					if (LegiScan::getConfig('want_bill_text'))
						$this->request('texts', $text['doc_id']);
				}
				// }}}

				// {{{ Amendments
				foreach ($bill['amendments'] as $amendment)
				{
					$sql = "INSERT INTO ls_bill_amendment (
								amendment_id, bill_id, local_copy, adopted,
								amendment_body_id, amendment_mime_id,
								amendment_date, amendment_title, amendment_desc,
								legiscan_url, state_url, amendment_size, amendment_hash,
								updated, created
							) VALUES (
								:amendment_id, :bill_id, :local_copy, :adopted,
								:amendment_body_id, :amendment_mime_id,
								:amendment_date, :amendment_title, :amendment_desc,
								:legiscan_url, :state_url, :amendment_size, :amendment_hash,
								:updated, :created
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':amendment_id', $amendment['amendment_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
					$stmt->bindValue(':adopted', $amendment['adopted'], PDO::PARAM_INT);
					$stmt->bindValue(':amendment_size', $amendment['amendment_size'], PDO::PARAM_INT);
					$stmt->bindValue(':amendment_hash', $amendment['amendment_hash'], PDO::PARAM_STR);
					$stmt->bindValue(':amendment_body_id', $amendment['chamber_id'], PDO::PARAM_INT);
					$stmt->bindValue(':amendment_mime_id', $amendment['mime_id'], PDO::PARAM_INT);
					$stmt->bindValue(':amendment_date', $this->dbDate($amendment['date']), PDO::PARAM_STR);
					$stmt->bindValue(':amendment_title', $amendment['title'], PDO::PARAM_STR);
					$stmt->bindValue(':amendment_desc', $amendment['description'], PDO::PARAM_STR);
					$stmt->bindValue(':legiscan_url', $amendment['url'], PDO::PARAM_STR);
					$stmt->bindValue(':state_url', $amendment['state_link'], PDO::PARAM_STR);
					$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
					$stmt->bindValue(':created', $now, PDO::PARAM_STR);
					$stmt->execute();

					if (LegiScan::getConfig('want_amendment'))
						$this->request('amendments', $amendment['amendment_id']);
				}
				// }}}

				// {{{ Supplements
				foreach ($bill['supplements'] as $supplement)
				{
					$sql = "INSERT INTO ls_bill_supplement (
								supplement_id, bill_id, local_copy,
								supplement_type_id, supplement_mime_id,
								supplement_date, supplement_title, supplement_desc,
								legiscan_url, state_url, supplement_size, supplement_hash,
								updated, created
							) VALUES (
								:supplement_id, :bill_id, :local_copy,
								:supplement_type_id, :supplement_mime_id,
								:supplement_date, :supplement_title, :supplement_desc,
								:legiscan_url, :state_url, :supplement_size, :supplement_hash,
								:updated, :created
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':supplement_id', $supplement['supplement_id'], PDO::PARAM_INT);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
					$stmt->bindValue(':supplement_size', $supplement['supplement_size'], PDO::PARAM_INT);
					$stmt->bindValue(':supplement_hash', $supplement['supplement_hash'], PDO::PARAM_STR);
					$stmt->bindValue(':supplement_type_id', $supplement['type_id'], PDO::PARAM_INT);
					$stmt->bindValue(':supplement_mime_id', $supplement['mime_id'], PDO::PARAM_INT);
					$stmt->bindValue(':supplement_date', $this->dbDate($supplement['date']), PDO::PARAM_INT);
					$stmt->bindValue(':supplement_title', $supplement['title'], PDO::PARAM_INT);
					$stmt->bindValue(':supplement_desc', $supplement['description'], PDO::PARAM_INT);
					$stmt->bindValue(':legiscan_url', $supplement['url'], PDO::PARAM_STR);
					$stmt->bindValue(':state_url', $supplement['state_link'], PDO::PARAM_STR);
					$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
					$stmt->bindValue(':created', $now, PDO::PARAM_STR);
					$stmt->execute();

					if (LegiScan::getConfig('want_supplement'))
						$this->request('supplements', $supplement['supplement_id']);
				}
				// }}}

				// {{{ Same As / Similar To
				foreach ($bill['sasts'] as $sast)
				{
					$sql = "INSERT INTO ls_bill_sast (
								bill_id, sast_type_id, sast_bill_id, sast_bill_number
							) VALUES (
								:bill_id, :sast_type_id, :sast_bill_id, :sast_bill_number
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':sast_type_id', $sast['type_id'], PDO::PARAM_INT);
					$stmt->bindValue(':sast_bill_id', $sast['sast_bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':sast_bill_number', $sast['sast_bill_number'], PDO::PARAM_STR);
					$stmt->execute();
				}
				// }}}

				// {{{ Subjects
				foreach ($bill['subjects'] as $subject)
				{
					$sql = "INSERT INTO ls_bill_subject (
								bill_id, subject_id
							) VALUES (
								:bill_id, :subject_id
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':subject_id', $subject['subject_id'], PDO::PARAM_INT);
					$stmt->execute();

					$subject_exists_id = $this->checkExists('subject', $subject['subject_id']);
					if (!$subject_exists_id)
					{
						$sql = "INSERT INTO ls_subject (
									subject_id, state_id, subject_name
								) VALUES (
									:subject_id, :state_id, :subject_name
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':subject_id', $subject['subject_id'], PDO::PARAM_INT);
						$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
						$stmt->bindValue(':subject_name', $subject['subject_name'], PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Calendar
				foreach ($bill['calendar'] as $calendar)
				{
					$event_hash = md5(strtolower("{$calendar['type']}-{$calendar['date']}-{$calendar['time']}-{$calendar['location']}-{$calendar['description']}"));
					$event_hash = substr($event_hash, 0, 8);

					$sql = "SELECT 1 FROM ls_bill_calendar WHERE bill_id = :bill_id AND event_hash = :event_hash";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':event_hash', $event_hash, PDO::PARAM_STR);
					$stmt->execute();
					if (!$stmt->fetchColumn())
					{
						$sql = "INSERT INTO ls_bill_calendar (
									bill_id, event_hash, event_type_id,	event_date,
									event_time, event_location, event_desc,
									updated, created
								) VALUES (
									:bill_id, :event_hash, :event_type_id, :event_date,
									:event_time, :event_location, :event_desc,
									:updated, :created
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':event_hash', $event_hash, PDO::PARAM_STR);
						$stmt->bindValue(':event_type_id', $calendar['type_id'], PDO::PARAM_INT);
						$stmt->bindValue(':event_date', $this->dbDate($calendar['date']), PDO::PARAM_STR);
						$stmt->bindValue(':event_time', $calendar['time'], PDO::PARAM_STR);
						$stmt->bindValue(':event_location', $calendar['location'], PDO::PARAM_STR);
						$stmt->bindValue(':event_desc', $calendar['description'], PDO::PARAM_STR);
						$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
						$stmt->bindValue(':created', $now, PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				// }}}
				// }}}
			}
			else
			{
				// Updating bills, a little less so... AKA more if statements

				// {{{ Update Bill
				$bill_old = $this->loadBill($bill['bill_id']);

				// {{{ Session
				$updates = array();

				if ($bill['session']['prefile'] != $bill_old['session']['prefile'])
					$updates['prefile'] = array($bill['session']['prefile'], PDO::PARAM_INT);
				if ($bill['session']['sine_die'] != $bill_old['session']['sine_die'])
					$updates['sine_die'] = array($bill['session']['sine_die'], PDO::PARAM_INT);
				if ($bill['session']['prior'] != $bill_old['session']['prior'])
					$updates['prior'] = array($bill['session']['prior'], PDO::PARAM_INT);
	
				if ($updates)
				{
					$key = array(
						'session_id'=>array($bill['session']['session_id'], PDO::PARAM_INT),
					);
					$stmt = $this->makeSQLStatement('update', 'ls_session', $updates, $key);
					$stmt->execute();
					
					$this->middlewareSignal('session', $bill['session_id']);
				}
				// }}}

				// {{{ Base Bill
				$updates = array();

				if ($bill['bill_type_id'] != $bill_old['bill_type_id'])
					$updates['bill_type_id'] = array($bill['bill_type_id'], PDO::PARAM_INT);
				if ($bill['current_body_id'] != $bill_old['current_body_id'])
					$updates['current_body_id'] = array($bill['current_body_id'], PDO::PARAM_INT);
				if ($bill['status'] != $bill_old['status'])
					$updates['status_id'] = array($bill['status'], PDO::PARAM_INT);
				if ($bill['status_date'] != $bill_old['status_date'])
					$updates['status_date'] = array($this->dbDate($bill['status_date']), PDO::PARAM_STR);
				if ($bill['title'] != $bill_old['title'])
					$updates['title'] = array($bill['title'], PDO::PARAM_STR);
				if ($bill['description'] != $bill_old['description'])
					$updates['description'] = array($bill['description'], PDO::PARAM_STR);
				if ($bill['pending_committee_id'] != $bill_old['pending_committee_id'])
					$updates['pending_committee_id'] = array($bill['pending_committee_id'], PDO::PARAM_INT);
				if ($bill['change_hash'] != $bill_old['change_hash'])
					$updates['change_hash'] = array($bill['change_hash'], PDO::PARAM_STR);

				if ($updates)
				{
					$key = array(
						'bill_id'=>array($bill['bill_id'], PDO::PARAM_INT),
					);
					$stmt = $this->makeSQLStatement('update', 'ls_bill', $updates, $key);
					$stmt->execute();
				}
				// }}}

				// {{{ New Committee Check
				if ($bill['pending_committee_id'] && !$this->checkExists('committee', $bill['pending_committee_id']))
				{
					$sql = "INSERT INTO ls_committee (
								committee_id, committee_body_id, committee_name
							) VALUES (
								:committee_id, :committee_body_id, :committee_name
							)";

					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':committee_id', $bill['committee']['committee_id'], PDO::PARAM_INT);
					$stmt->bindValue(':committee_body_id', $bill['committee']['chamber_id'], PDO::PARAM_INT);
					$stmt->bindValue(':committee_name', $bill['committee']['name'], PDO::PARAM_STR);
					$stmt->execute();
				}
				// }}}

				// {{{ Referral Chain
				if (!empty($bill['referrals']))
				{
					foreach ($bill['referrals'] as $idx => $link)
					{
						if (!$this->checkExists('committee', $link['committee_id']))
						{
							$sql = "INSERT INTO ls_committee (
								committee_id, committee_body_id, committee_name
							) VALUES (
								:committee_id, :committee_body_id, :committee_name
							)";

							$stmt = $this->db->prepare($sql);
							$stmt->bindValue(':committee_id', $link['committee_id'], PDO::PARAM_INT);
							$stmt->bindValue(':committee_body_id', $link['chamber_id'], PDO::PARAM_INT);
							$stmt->bindValue(':committee_name', $link['name'], PDO::PARAM_STR);
							$stmt->execute();
						}

						$step = $idx + 1;
						$updates = array();
						$key = array(
							'bill_id' => array($bill['bill_id'], PDO::PARAM_INT),
							'referral_step' => array($step, PDO::PARAM_INT),
						);

						if (isset($bill_old['referrals'][$idx]))
						{
							if ($link['date'] != $bill_old['referrals'][$idx]['date'])
								$updates['referral_date'] = array($this->dbDate($link['date']), PDO::PARAM_STR);
							if ($link['committee_id'] != $bill_old['referrals'][$idx]['committee_id'])
								$updates['committee_id'] = array($link['committee_id'], PDO::PARAM_INT);
							if ($updates)
							{
								$stmt = $this->makeSQLStatement('update', 'ls_bill_referral', $updates, $key);
								$stmt->execute();
							}
						}
						else
						{
							$updates['referral_date'] = array($this->dbDate($link['date']), PDO::PARAM_STR);
							$updates['committee_id'] = array($link['committee_id'], PDO::PARAM_STR);
							$stmt = $this->makeSQLStatement('insert', 'ls_bill_referral', $updates, $key);
							$stmt->execute();
						}
					}
					// Check for shrinkage, nobody likes shrinkage
					if (($cnt = count($bill['referrals'])) < ($ocnt = count($bill_old['referrals'])))
					{
						$sql = 'DELETE FROM ls_bill_referral WHERE bill_id = :bill_id AND referral_step > :step_cut';
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':step_cut', $cnt, PDO::PARAM_INT);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Reasons
				if (isset($bill['reasons']))
				{
					foreach ($bill['reasons'] as $reason_id => $reason)
					{
						$sql = "INSERT INTO ls_bill_reason (
									bill_id, reason_id, created
								) VALUES (
									:bill_id, :reason_id, :created
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':reason_id', $reason_id, PDO::PARAM_INT);
						$stmt->bindValue(':created', $now, PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Progress
				foreach ($bill['progress'] as $idx => $event)
				{
					$step = $idx + 1;
					$updates = array();
					$key = array(
						'bill_id' => array($bill['bill_id'], PDO::PARAM_INT),
						'progress_step' => array($step, PDO::PARAM_INT),
					);
					if (isset($bill_old['progress'][$idx]))
					{
						if ($event['date'] != $bill_old['progress'][$idx]['date'])
							$updates['progress_date'] = array($this->dbDate($event['date']), PDO::PARAM_STR);
						if ($event['event'] != $bill_old['progress'][$idx]['event'])
							$updates['progress_event_id'] = array($event['event'], PDO::PARAM_INT);
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_progress', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						$updates['progress_date'] = array($this->dbDate($event['date']), PDO::PARAM_STR);
						$updates['progress_event_id'] = array($event['event'], PDO::PARAM_INT);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_progress', $updates, $key);
						$stmt->execute();
					}
				}
				// Check for shrinkage, nobody likes shrinkage
				if (($cnt = count($bill['progress'])) < ($ocnt = count($bill_old['progress'])))
				{
					$sql = 'DELETE FROM ls_bill_progress WHERE bill_id = :bill_id AND progress_step > :step_cut';
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':step_cut', $cnt, PDO::PARAM_INT);
					$stmt->execute();
				}
				// }}}

				// {{{ History
				foreach ($bill['history'] as $idx => $action)
				{
					$step = $idx + 1;
					$updates = array();
					$key = array(
						'bill_id' => array($bill['bill_id'], PDO::PARAM_INT),
						'history_step' => array($step, PDO::PARAM_INT),
					);
					if (isset($bill_old['history'][$idx]))
					{
						if ($action['importance'] != $bill_old['history'][$idx]['importance'])
							$updates['history_major'] = array($action['importance'], PDO::PARAM_INT);
						if ($action['chamber_id'] != $bill_old['history'][$idx]['chamber_id'])
							$updates['history_body_id'] = array($action['chamber_id'], PDO::PARAM_INT);
						if ($action['date'] != $bill_old['history'][$idx]['date'])
							$updates['history_date'] = array($this->dbDate($action['date']), PDO::PARAM_STR);
						if ($action['action'] != $bill_old['history'][$idx]['action'])
							$updates['history_action'] = array($action['action'], PDO::PARAM_STR);
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_history', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						$updates['history_major'] = array($action['importance'], PDO::PARAM_INT);
						$updates['history_body_id'] = array($action['chamber_id'], PDO::PARAM_INT);
						$updates['history_date'] = array($this->dbDate($action['date']), PDO::PARAM_STR);
						$updates['history_action'] = array($action['action'], PDO::PARAM_STR);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_history', $updates, $key);
						$stmt->execute();
					}
				}
				// Check for shrinkage, nobody likes shrinkage
				if (($cnt = count($bill['history'])) < ($ocnt = count($bill_old['history'])))
				{
					$sql = 'DELETE FROM ls_bill_history WHERE bill_id = :bill_id AND history_step > :step_cut';
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':step_cut', $cnt, PDO::PARAM_INT);
					$stmt->execute();
				}
				// }}}

				// {{{ Sponsors
				foreach ($bill['sponsors'] as $idx => $person)
				{
					$exists = false;
					$updates = array();
					$key = array(
						'bill_id' => array($bill['bill_id'], PDO::PARAM_INT),
						'people_id' => array($person['people_id'], PDO::PARAM_INT),
					);

					foreach ($bill_old['sponsors'] as $og_idx => $og)
					{
						if ($person['people_id'] == $og['people_id'])
						{
							$exists = true;
							break;
						}
					}

					if (!$exists)
					{
						$updates['sponsor_order'] = array($person['sponsor_order'], PDO::PARAM_INT);
						$updates['sponsor_type_id'] = array($person['sponsor_type_id'], PDO::PARAM_INT);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_sponsor', $updates, $key);
						$stmt->execute();
					}
					else
					{
						if ($person['sponsor_order'] != $bill_old['sponsors'][$og_idx]['sponsor_order'])
							$updates['sponsor_order'] = array($person['sponsor_order'], PDO::PARAM_INT);
						if ($person['sponsor_type_id'] != $bill_old['sponsors'][$og_idx]['sponsor_type_id'])
							$updates['sponsor_type_id'] = array($person['sponsor_type_id'], PDO::PARAM_INT);
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_sponsor', $updates, $key);
							$stmt->execute();
						}
					}

					// Keep people records updated as we see them
					if (($people_exists_id = $this->checkExists('people', $person['people_id'])))
						$sql = "UPDATE ls_people
								SET state_id = :state_id, role_id = :role_id, party_id = :party_id,
									name = :name, first_name = :first_name, middle_name = :middle_name,
									last_name = :last_name, suffix = :suffix, nickname = :nickname,
									district = :district, committee_sponsor_id = :committee_id,
									votesmart_id = :votesmart_id, followthemoney_eid = :followthemoney_eid,
									opensecrets_id = :opensecrets_id, ballotpedia = :ballotpedia,
									knowwho_pid = :knowwho_pid, person_hash = :person_hash,
									updated = :updated
								WHERE people_id = :people_id";
					else
						$sql = "INSERT INTO ls_people (
									people_id, state_id, role_id, party_id, name, first_name,
									middle_name, last_name, suffix, nickname, district,
									committee_sponsor_id, votesmart_id, followthemoney_eid,
									opensecrets_id, ballotpedia, knowwho_pid, person_hash,
									updated, created
								) VALUES (
									:people_id, :state_id, :role_id, :party_id, :name, :first_name,
									:middle_name, :last_name, :suffix, :nickname, :district,
									:committee_id, :votesmart_id, :followthemoney_eid,
									:opensecrets_id, :ballotpedia, :knowwho_pid, :person_hash,
									:updated, :created
								)";

					if ($people_exists_id)
						$p = $this->db->query("SELECT person_hash FROM ls_people WHERE people_id = {$people_exists_id}")->fetch();

					if (!$people_exists_id || $p['person_hash'] != $person['person_hash'])
					{
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':people_id', $person['people_id'], PDO::PARAM_INT);
						$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
						$stmt->bindValue(':role_id', $person['role_id'], PDO::PARAM_INT);
						$stmt->bindValue(':party_id', $person['party_id'], PDO::PARAM_INT);
						$stmt->bindValue(':name', $person['name'], PDO::PARAM_STR);
						$stmt->bindValue(':first_name', $person['first_name'], PDO::PARAM_STR);
						$stmt->bindValue(':middle_name', $person['middle_name'], PDO::PARAM_STR);
						$stmt->bindValue(':last_name', $person['last_name'], PDO::PARAM_STR);
						$stmt->bindValue(':suffix', $person['suffix'], PDO::PARAM_STR);
						$stmt->bindValue(':nickname', $person['nickname'], PDO::PARAM_STR);
						$stmt->bindValue(':district', $person['district'], PDO::PARAM_STR);
						$stmt->bindValue(':committee_id', $person['committee_id'], PDO::PARAM_INT);
						$stmt->bindValue(':ballotpedia', $person['ballotpedia'], PDO::PARAM_STR);
						$stmt->bindValue(':followthemoney_eid', $person['ftm_eid'], PDO::PARAM_INT);
						$stmt->bindValue(':votesmart_id', $person['votesmart_id'], PDO::PARAM_INT);
						$stmt->bindValue(':knowwho_pid', $person['knowwho_pid'], PDO::PARAM_INT);
						$stmt->bindValue(':opensecrets_id', $person['opensecrets_id'], PDO::PARAM_STR);
						$stmt->bindValue(':person_hash', $person['person_hash'], PDO::PARAM_STR);
						$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
						if (!$people_exists_id)
							$stmt->bindValue(':created', $now, PDO::PARAM_STR);

						$stmt->execute();
					}
				}

				// Cleanup sponsors that may have been removed
				foreach ($bill_old['sponsors'] as $og)
				{
					$exists = false;
					foreach ($bill['sponsors'] as $person)
					{
						if ($og['people_id'] == $person['people_id'])
							$exists = true;
					}
					if (!$exists)
					{
						$sql = 'DELETE FROM ls_bill_sponsor WHERE bill_id = :bill_id AND people_id = :people_id';
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':people_id', $og['people_id'], PDO::PARAM_INT);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Votes
				foreach ($bill['votes'] as $vote)
				{
					$updates = array();
					$key = array(
						'roll_call_id' => array($vote['roll_call_id'], PDO::PARAM_INT),
					);
					if (($exists_id = $this->checkExists('bill_vote', $vote['roll_call_id'])))
					{
						// See if anything has changed with an existing record
						foreach ($bill_old['votes'] as $old_vote)
						{
							// Found our match
							if ($vote['roll_call_id'] == $old_vote['roll_call_id'])
							{
								if ($vote['chamber_id'] != $old_vote['chamber_id'])
									$updates['roll_call_body_id'] = array($vote['chamber_id'], PDO::PARAM_INT);
								if ($vote['date'] != $old_vote['date'])
									$updates['roll_call_date'] = array($this->dbDate($vote['date']), PDO::PARAM_STR);
								if ($vote['desc'] != $old_vote['desc'])
									$updates['roll_call_desc'] = array($vote['desc'], PDO::PARAM_STR);
								if ($vote['yea'] != $old_vote['yea'])
									$updates['yea'] = array($vote['yea'], PDO::PARAM_INT);
								if ($vote['nay'] != $old_vote['nay'])
									$updates['nay'] = array($vote['nay'], PDO::PARAM_INT);
								if ($vote['nv'] != $old_vote['nv'])
									$updates['nv'] = array($vote['nv'], PDO::PARAM_INT);
								if ($vote['absent'] != $old_vote['absent'])
									$updates['absent'] = array($vote['absent'], PDO::PARAM_INT);
								if ($vote['total'] != $old_vote['total'])
									$updates['total'] = array($vote['total'], PDO::PARAM_INT);
								if ($vote['passed'] != $old_vote['passed'])
									$updates['passed'] = array($vote['passed'], PDO::PARAM_INT);
								if ($vote['url'] != $old_vote['url'])
									$updates['legiscan_url'] = array($vote['url'], PDO::PARAM_STR);
								if ($vote['state_link'] != $old_vote['state_link'])
									$updates['state_url'] = array($vote['state_link'], PDO::PARAM_STR);
								break;
							}
						}
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_vote', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						// New roll call
						$updates['roll_call_id'] = array($vote['roll_call_id'], PDO::PARAM_INT);
						$updates['bill_id'] = array($bill['bill_id'], PDO::PARAM_INT);
						$updates['roll_call_body_id'] = array($vote['chamber_id'], PDO::PARAM_INT);
						$updates['roll_call_date'] = array($this->dbDate($vote['date']), PDO::PARAM_STR);
						$updates['roll_call_desc'] = array($vote['desc'], PDO::PARAM_STR);
						$updates['yea'] = array($vote['yea'], PDO::PARAM_INT);
						$updates['nay'] = array($vote['nay'], PDO::PARAM_INT);
						$updates['nv'] = array($vote['nv'], PDO::PARAM_INT);
						$updates['absent'] = array($vote['absent'], PDO::PARAM_INT);
						$updates['total'] = array($vote['total'], PDO::PARAM_INT);
						$updates['passed'] = array($vote['passed'], PDO::PARAM_INT);
						$updates['legiscan_url'] = array($vote['url'], PDO::PARAM_STR);
						$updates['state_url'] = array($vote['state_link'], PDO::PARAM_STR);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_vote', $updates, $key);
						$stmt->execute();

						// If this is new and we want vote details, add to missing list
						if (LegiScan::getConfig('want_vote_details'))
							$this->request('votes', $vote['roll_call_id']);
					}
				}
				// }}}

				// {{{ Texts
				foreach ($bill['texts'] as $text)
				{
					$updates = array();
					$key = array(
						'text_id' => array($text['doc_id'], PDO::PARAM_INT),
					);
					if (($exists_id = $this->checkExists('bill_text', $text['doc_id'])))
					{
						// See if anything has changed with an existing record
						foreach ($bill_old['texts'] as $old_text)
						{
							// Found our match
							if ($text['doc_id'] == $old_text['doc_id'])
							{
								if ($text['date'] != $old_text['date'])
									$updates['bill_text_date'] = array($this->dbDate($text['date']), PDO::PARAM_STR);
								if ($text['type_id'] != $old_text['type_id'])
									$updates['bill_text_type_id'] = array($text['type_id'], PDO::PARAM_INT);
								if ($text['mime_id'] != $old_text['mime_id'])
									$updates['bill_text_mime_id'] = array($text['mime_id'], PDO::PARAM_INT);
								if ($text['url'] != $old_text['url'])
									$updates['legiscan_url'] = array($text['url'], PDO::PARAM_STR);
								if ($text['state_link'] != $old_text['state_link'])
									$updates['state_url'] = array($text['state_link'], PDO::PARAM_STR);
								if ($text['text_size'] != $old_text['text_size'])
									$updates['bill_text_size'] = array($text['text_size'], PDO::PARAM_INT);
								if ($text['text_hash'] != $old_text['text_hash'])
									$updates['bill_text_hash'] = array($text['text_hash'], PDO::PARAM_STR);
								break;
							}
						}
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_text', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						// New bill text
						$updates['text_id'] = array($text['doc_id'], PDO::PARAM_INT);
						$updates['bill_id'] = array($bill['bill_id'], PDO::PARAM_INT);
						$updates['local_copy'] = array(0, PDO::PARAM_INT);
						$updates['bill_text_size'] = array($text['text_size'], PDO::PARAM_INT);
						$updates['bill_text_hash'] = array($text['text_hash'], PDO::PARAM_STR);
						$updates['bill_text_type_id'] = array($text['type_id'], PDO::PARAM_INT);
						$updates['bill_text_mime_id'] = array($text['mime_id'], PDO::PARAM_INT);
						$updates['bill_text_date'] = array($this->dbDate($text['date']), PDO::PARAM_STR);
						$updates['legiscan_url'] = array($text['url'], PDO::PARAM_STR);
						$updates['state_url'] = array($text['state_link'], PDO::PARAM_STR);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_text', $updates, $key);
						$stmt->execute();

						// If this is new and we want texts, add to missing list
						if (LegiScan::getConfig('want_bill_text'))
							$this->request('texts', $text['doc_id']);
					}
				}
				// }}}

				// {{{ Amendments
				foreach ($bill['amendments'] as $amendment)
				{
					$updates = array();
					$key = array(
						'amendment_id' => array($amendment['amendment_id'], PDO::PARAM_INT),
					);
					if (($exists_id = $this->checkExists('bill_amendment', $amendment['amendment_id'])))
					{
						// See if anything has changed with an existing record
						foreach ($bill_old['amendments'] as $old_amendment)
						{
							// Found our match
							if ($amendment['amendment_id'] == $old_amendment['amendment_id'])
							{
								if ($amendment['adopted'] != $old_amendment['adopted'])
									$updates['adopted'] = array($amendment['adopted'], PDO::PARAM_INT);
								if ($amendment['chamber_id'] != $old_amendment['chamber_id'])
									$updates['amendment_body_id'] = array($amendment['chamber_id'], PDO::PARAM_INT);
								if ($amendment['mime_id'] != $old_amendment['mime_id'])
									$updates['amendment_mime_id'] = array($amendment['mime_id'], PDO::PARAM_INT);
								if ($amendment['date'] != $old_amendment['date'])
									$updates['amendment_date'] = array($this->dbDate($amendment['date']), PDO::PARAM_STR);
								if ($amendment['title'] != $old_amendment['title'])
									$updates['amendment_title'] = array($amendment['title'], PDO::PARAM_STR);
								if ($amendment['description'] != $old_amendment['description'])
									$updates['amendment_desc'] = array($amendment['description'], PDO::PARAM_STR);
								if ($amendment['url'] != $old_amendment['url'])
									$updates['legiscan_url'] = array($amendment['url'], PDO::PARAM_STR);
								if ($amendment['state_link'] != $old_amendment['state_link'])
									$updates['state_url'] = array($amendment['state_link'], PDO::PARAM_STR);
								if ($amendment['amendment_size'] != $old_amendment['amendment_size'])
									$updates['amendment_size'] = array($amendment['amendment_size'], PDO::PARAM_INT);
								if ($amendment['amendment_hash'] != $old_amendment['amendment_hash'])
									$updates['amendment_hash'] = array($amendment['amendment_hash'], PDO::PARAM_STR);
								break;
							}
						}
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_amendment', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						// New bill amendment
						$updates['amendment_id'] = array($amendment['amendment_id'], PDO::PARAM_INT);
						$updates['bill_id'] = array($bill['bill_id'], PDO::PARAM_INT);
						$updates['local_copy'] = array(0, PDO::PARAM_INT);
						$updates['adopted'] = array($amendment['adopted'], PDO::PARAM_INT);
						$updates['amendment_size'] = array($amendment['amendment_size'], PDO::PARAM_INT);
						$updates['amendment_hash'] = array($amendment['amendment_hash'], PDO::PARAM_STR);
						$updates['amendment_body_id'] = array($amendment['chamber_id'], PDO::PARAM_INT);
						$updates['amendment_mime_id'] = array($amendment['mime_id'], PDO::PARAM_INT);
						$updates['amendment_date'] = array($this->dbDate($amendment['date']), PDO::PARAM_STR);
						$updates['amendment_title'] = array($amendment['title'], PDO::PARAM_STR);
						$updates['amendment_desc'] = array($amendment['description'], PDO::PARAM_STR);
						$updates['legiscan_url'] = array($amendment['url'], PDO::PARAM_STR);
						$updates['state_url'] = array($amendment['state_link'], PDO::PARAM_STR);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_amendment', $updates, $key);
						$stmt->execute();

						// If this is new and we want amendments, add to missing list
						if (LegiScan::getConfig('want_amendment'))
							$this->request('amendments', $amendment['amendment_id']);
					}
				}
				// }}}

				// {{{ Supplements
				foreach ($bill['supplements'] as $supplement)
				{
					$updates = array();
					$key = array(
						'supplement_id' => array($supplement['supplement_id'], PDO::PARAM_INT),
					);
					if (($exists_id = $this->checkExists('bill_supplement', $supplement['supplement_id'])))
					{
						// See if anything has changed with an existing record
						foreach ($bill_old['supplements'] as $old_supplement)
						{
							// Found our match
							if ($supplement['supplement_id'] == $old_supplement['supplement_id'])
							{
								if ($supplement['type_id'] != $old_supplement['type_id'])
									$updates['supplement_type_id'] = array($supplement['type_id'], PDO::PARAM_INT);
								if ($supplement['mime_id'] != $old_supplement['mime_id'])
									$updates['supplement_mime_id'] = array($supplement['mime_id'], PDO::PARAM_INT);
								if ($supplement['date'] != $old_supplement['date'])
									$updates['supplement_date'] = array($this->dbDate($supplement['date']), PDO::PARAM_STR);
								if ($supplement['title'] != $old_supplement['title'])
									$updates['supplement_title'] = array($supplement['title'], PDO::PARAM_STR);
								if ($supplement['description'] != $old_supplement['description'])
									$updates['supplement_desc'] = array($supplement['description'], PDO::PARAM_STR);
								if ($supplement['url'] != $old_supplement['url'])
									$updates['legiscan_url'] = array($supplement['url'], PDO::PARAM_STR);
								if ($supplement['state_link'] != $old_supplement['state_link'])
									$updates['state_url'] = array($supplement['state_link'], PDO::PARAM_STR);
								if ($supplement['supplement_size'] != $old_supplement['supplement_size'])
									$updates['supplement_size'] = array($supplement['supplement_size'], PDO::PARAM_INT);
								if ($supplement['supplement_hash'] != $old_supplement['supplement_hash'])
									$updates['supplement_hash'] = array($supplement['supplement_hash'], PDO::PARAM_STR);
								break;
							}
						}
						if ($updates)
						{
							$stmt = $this->makeSQLStatement('update', 'ls_bill_supplement', $updates, $key);
							$stmt->execute();
						}
					}
					else
					{
						// New bill supplement
						$updates['supplement_id'] = array($supplement['supplement_id'], PDO::PARAM_INT);
						$updates['bill_id'] = array($bill['bill_id'], PDO::PARAM_INT);
						$updates['local_copy'] = array(0, PDO::PARAM_INT);
						$updates['supplement_size'] = array($supplement['supplement_size'], PDO::PARAM_INT);
						$updates['supplement_hash'] = array($supplement['supplement_hash'], PDO::PARAM_STR);
						$updates['supplement_type_id'] = array($supplement['type_id'], PDO::PARAM_INT);
						$updates['supplement_mime_id'] = array($supplement['mime_id'], PDO::PARAM_INT);
						$updates['supplement_date'] = array($this->dbDate($supplement['date']), PDO::PARAM_INT);
						$updates['supplement_title'] = array($supplement['title'], PDO::PARAM_INT);
						$updates['supplement_desc'] = array($supplement['description'], PDO::PARAM_STR);
						$updates['legiscan_url'] = array($supplement['url'], PDO::PARAM_STR);
						$updates['state_url'] = array($supplement['state_link'], PDO::PARAM_STR);
						$stmt = $this->makeSQLStatement('insert', 'ls_bill_supplement', $updates, $key);
						$stmt->execute();

						// If this is new and we want supplements, add to missing list
						if (LegiScan::getConfig('want_supplement'))
							$this->request('supplements', $supplement['supplement_id']);
					}
				}
				// }}}

				// {{{ Same As / Similar To
				$sast_ids = $old_sast_ids = array();
				foreach ($bill['sasts'] as $sast)
					$sast_ids[] = $sast['sast_bill_id'];
				foreach ($bill_old['sasts'] as $old_sast)
					$old_sast_ids[] = $old_sast['sast_bill_id'];
				foreach ($bill['sasts'] as $sast)
				{
					if (!in_array($sast['sast_bill_id'], $old_sast_ids))
					{
						$sql = "INSERT INTO ls_bill_sast (
									bill_id, sast_type_id, sast_bill_id, sast_bill_number
								) VALUES (
									:bill_id, :sast_type_id, :sast_bill_id, :sast_bill_number
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':sast_type_id', $sast['type_id'], PDO::PARAM_INT);
						$stmt->bindValue(':sast_bill_id', $sast['sast_bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':sast_bill_number', $sast['sast_bill_number'], PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				foreach ($bill_old['sasts'] as $old_sast)
				{
					if (!in_array($old_sast['sast_bill_id'], $sast_ids))
					{
						$sql = "DELETE FROM ls_bill_sast WHERE bill_id = :bill_id AND sast_bill_id = :sast_bill_id AND sast_type_id = :sast_type_id";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':sast_bill_id', $old_sast['sast_bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':sast_type_id', $old_sast['sast_type_id'], PDO::PARAM_INT);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Subject
				$subject_ids = $old_subject_ids = array();
				foreach ($bill['subjects'] as $subject)
					$subject_ids[] = $subject['subject_id'];
				foreach ($bill_old['subjects'] as $old_subject)
					$old_subject_ids[] = $old_subject['subject_id'];
				foreach ($bill['subjects'] as $subject)
				{
					if (!in_array($subject['subject_id'], $old_subject_ids))
					{
						$sql = "INSERT INTO ls_bill_subject (
									bill_id, subject_id
								) VALUES (
									:bill_id, :subject_id
								)";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':subject_id', $subject['subject_id'], PDO::PARAM_INT);
						$stmt->execute();

						$subject_exists_id = $this->checkExists('subject', $subject['subject_id']);
						if (!$subject_exists_id)
						{
							$sql = "INSERT INTO ls_subject (
										subject_id, state_id, subject_name
									) VALUES (
										:subject_id, :state_id, :subject_name
									)";

							$stmt = $this->db->prepare($sql);
							$stmt->bindValue(':subject_id', $subject['subject_id'], PDO::PARAM_INT);
							$stmt->bindValue(':state_id', $bill['state_id'], PDO::PARAM_INT);
							$stmt->bindValue(':subject_name', $subject['subject_name'], PDO::PARAM_STR);
							$stmt->execute();
						}
					}
				}
				foreach ($bill_old['subjects'] as $old_subject)
				{
					if (!in_array($old_subject['subject_id'], $subject_ids))
					{
						$sql = "DELETE FROM ls_bill_subject WHERE bill_id = :bill_id AND subject_id = :subject_id";

						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':subject_id', $old_subject['subject_id'], PDO::PARAM_INT);
						$stmt->execute();
					}
				}
				// }}}

				// {{{ Calendar
				$event_hashes = array();
				foreach ($bill['calendar'] as $cal)
				{
					$event_hash = md5(strtolower("{$cal['type']}-{$cal['date']}-{$cal['time']}-{$cal['location']}-{$cal['description']}"));
					$event_hash = substr($event_hash, 0, 8);
					$event_hashes[$event_hash] = $event_hash;

					// Avoid a hash collision
					$sql = "SELECT 1 FROM ls_bill_calendar WHERE bill_id = :bill_id AND event_hash = :event_hash";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':event_hash', $event_hash, PDO::PARAM_STR);
					$stmt->execute();

					if (!$stmt->fetchColumn())
					{
						$key = array(
							'bill_id' => array($bill['bill_id'], PDO::PARAM_INT),
							'event_hash' => array($event_hash, PDO::PARAM_STR),
						);
						
						$updates = array();
						$updates['event_type_id'] = array($cal['type_id'], PDO::PARAM_INT);
						$updates['event_date'] = array($this->dbDate($cal['date']), PDO::PARAM_STR);
						$updates['event_time'] = array($cal['time'], PDO::PARAM_STR);
						$updates['event_location'] = array($cal['location'], PDO::PARAM_STR);
						$updates['event_desc'] = array($cal['description'], PDO::PARAM_STR);

						$stmt = $this->makeSQLStatement('insert', 'ls_bill_calendar', $updates, $key);
						$stmt->execute();
					}
				}

				// Cleanup any events that are not in the current list of event_hashes
				foreach ($bill_old['calendar'] as $cal)
				{
					if (!isset($event_hashes[$cal['event_hash']]))
					{
						$sql = "DELETE FROM ls_bill_calendar WHERE bill_id = :bill_id AND event_hash = :event_hash";
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':event_hash', $cal['event_hash'], PDO::PARAM_STR);
						$stmt->execute();
					}
				}
				// }}}
				// }}}
			}

			$this->db->commit();

			$this->middlewareSignal('bill', $bill['bill_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		}

		if (!empty($this->missing))
			return $this->missing;
		else
			return array();
	}
	// }}}

	// {{{ processBillText()
	/** 
	 * Process a bill text payload
	 *
	 * @throws APIException
	 * @throws APIAccessException
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a bill text
	 * 
	 * @return boolean
	 *   Indicating the success/failure of bill text payload processing
	 *
	 */
	public function processBillText($payload)
	{
		$this->resetMissing(); // For consistency

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processBillText payload status = "' . $payload['status'] . '"');

		$text = $payload['text'];

		$now = date('Y-m-d H:i:s');
			
		try {
			if (($exists_id = $this->checkExists('bill_text', $text['doc_id'])))
				$sql = "UPDATE ls_bill_text
						SET bill_id = :bill_id,
							local_copy = :local_copy,
							bill_text_type_id = :bill_text_type_id,
							bill_text_mime_id = :bill_text_mime_id, 
							bill_text_date = :bill_text_date,
							bill_text_size = :bill_text_size,
							bill_text_hash = :bill_text_hash,
							updated = :updated 
						WHERE text_id = :text_id";
			else
				$sql = "INSERT INTO ls_bill_text (
							text_id, bill_id, local_copy, bill_text_type_id,
							bill_text_mime_id, bill_text_date, bill_text_size, bill_text_hash,
							updated, created
						) VALUES (
							:text_id, :bill_id, :local_copy, :bill_text_type_id,
							:bill_text_mime_id, :bill_text_date, :bill_text_size, :bill_text_hash,
							:updated, :created
						)";

			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':text_id', $text['doc_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_id', $text['bill_id'], PDO::PARAM_INT);
			$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
			$stmt->bindValue(':bill_text_size', $text['text_size'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_text_hash', $text['text_hash'], PDO::PARAM_STR);
			$stmt->bindValue(':bill_text_type_id', $text['type_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_text_mime_id', $text['mime_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_text_date', $this->dbDate($text['date']), PDO::PARAM_STR);
			$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
			if (!$exists_id)
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			if ($text['doc'] != '')
			{
				$blob = base64_decode($text['doc']);
				if ($blob !== false)
				{
					$cache_file = $this->getCacheFilename('text', $text['doc_id']);
					$this->cache->set($cache_file, $blob);
					$sql = "UPDATE ls_bill_text
							SET local_copy = :local_copy, local_fragment = :local_fragment
							WHERE text_id = :text_id";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':local_copy', 1, PDO::PARAM_INT);
					$stmt->bindValue(':local_fragment', $cache_file, PDO::PARAM_STR);
					$stmt->bindValue(':text_id', $text['doc_id'], PDO::PARAM_INT);
					$stmt->execute();	
				}
				else
				{
					throw new APIException('Could not decode bill text blob');
				}
			}

			$this->db->commit();

			$this->middlewareSignal('text', $text['doc_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		} catch (APIAccessException $e) {
			$this->db->rollback();

			throw($e);
		} catch (APIException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processAmendment()
	/** 
	 * Process an amendment payload
	 *
	 * @throws APIException
	 * @throws APIAccessException
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing an amendment
	 * 
	 * @return boolean
	 *   Indicating the success/failure of amendment payload processing
	 *
	 */
	public function processAmendment($payload)
	{
		$this->resetMissing(); // For consistency

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processAmendment payload status = "' . $payload['status'] . '"');

		$amendment = $payload['amendment'];

		try {
			if (($exists_id = $this->checkExists('bill_amendment', $amendment['amendment_id'])))
				$sql = "UPDATE ls_bill_amendment
						SET bill_id = :bill_id,
							local_copy = :local_copy,
							adopted = :adopted,
							amendment_body_id = :amendment_body_id,
							amendment_mime_id = :amendment_mime_id,
							amendment_date = :amendment_date,
							amendment_title = :amendment_title,
							amendment_desc = :amendment_desc,
							amendment_size = :amendment_size,
							amendment_hash = :amendment_hash,
							updated = :updated
						WHERE amendment_id = :amendment_id";
			else
				$sql = "INSERT INTO ls_bill_amendment (
							amendment_id, bill_id, local_copy, adopted,
							amendment_body_id, amendment_mime_id, amendment_date,
							amendment_title, amendment_desc, amendment_size, amendment_hash,
							updated, created
						) VALUES (
							:amendment_id, :bill_id, :local_copy, :adopted,
							:amendment_body_id, :amendment_mime_id, :amendment_date,
							:amendment_title, :amendment_desc, :amendment_size, :amendment_hash,
							:updated, :created
						)";

			$now = date('Y-m-d H:i:s');
			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':amendment_id', $amendment['amendment_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_id', $amendment['bill_id'], PDO::PARAM_INT);
			$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
			$stmt->bindValue(':adopted', $amendment['adopted'], PDO::PARAM_INT);
			$stmt->bindValue(':amendment_size', $amendment['amendment_size'], PDO::PARAM_INT);
			$stmt->bindValue(':amendment_hash', $amendment['amendment_hash'], PDO::PARAM_STR);
			$stmt->bindValue(':amendment_body_id', $amendment['chamber_id'], PDO::PARAM_INT);
			$stmt->bindValue(':amendment_mime_id', $amendment['mime_id'], PDO::PARAM_INT);
			$stmt->bindValue(':amendment_date', $this->dbDate($amendment['date']), PDO::PARAM_STR);
			$stmt->bindValue(':amendment_title', $amendment['title'], PDO::PARAM_STR);
			$stmt->bindValue(':amendment_desc', $amendment['description'], PDO::PARAM_STR);
			$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
			if (!$exists_id)
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);

			$stmt->execute();

			if ($amendment['doc'] != '')
			{
				$blob = base64_decode($amendment['doc']);
				if ($blob !== false)
				{
					$cache_file = $this->getCacheFilename('amendment', $amendment['amendment_id']);
					$this->cache->set($cache_file, $blob);
					$sql = "UPDATE ls_bill_amendment
							SET local_copy = :local_copy, local_fragment = :local_fragment
							WHERE amendment_id = :amendment_id";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':local_copy', 1, PDO::PARAM_INT);
					$stmt->bindValue(':local_fragment', $cache_file, PDO::PARAM_STR);
					$stmt->bindValue(':amendment_id', $amendment['amendment_id'], PDO::PARAM_INT);
					$stmt->execute();	
				}
				else
				{
					throw new APIException('Could not decode amendment text blob');
				}
			}

			$this->db->commit();

			$this->middlewareSignal('amendment', $amendment['amendment_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		} catch (APIAccessException $e) {
			$this->db->rollback();

			throw($e);
		} catch (APIException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processSupplement()
	/** 
	 * Process a supplement payload
	 *
	 * @throws APIException
	 * @throws APIAccessException
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a supplement
	 * 
	 * @return boolean
	 *   Indicating the success/failure of supplement payload processing
	 */
	public function processSupplement($payload)
	{
		$this->resetMissing(); // For consistency

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processSupplement payload status = "' . $payload['status'] . '"');

		$supplement = $payload['supplement'];

		try {
			if (($exists_id = $this->checkExists('bill_supplement', $supplement['supplement_id'])))
				$sql = "UPDATE ls_bill_supplement
						SET bill_id = :bill_id, local_copy = :local_copy,
							supplement_type_id = :supplement_type_id,
							supplement_mime_id = :supplement_mime_id,
							supplement_date = :supplement_date,
							supplement_title = :supplement_title,
							supplement_desc = :supplement_desc,
							supplement_size = :supplement_size,
							supplement_hash = :supplement_hash,
							updated = :updated
						WHERE supplement_id = :supplement_id";
			else
				$sql = "INSERT INTO ls_bill_supplement (
							supplement_id, bill_id, local_copy, supplement_type_id,
							supplement_mime_id, supplement_date, supplement_title,
							supplement_desc, supplement_size, supplement_hash,
							updated, created
						) VALUES (
							:supplement_id, :bill_id, :local_copy, :supplement_type_id,
							:supplement_mime_id, :supplement_date, :supplement_title,
							:supplement_desc, :supplement_size, :supplement_hash,
							:updated, :created
						)";

			$now = date('Y-m-d H:i:s');

			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':supplement_id', $supplement['supplement_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_id', $supplement['bill_id'], PDO::PARAM_INT);
			$stmt->bindValue(':local_copy', 0, PDO::PARAM_INT);
			$stmt->bindValue(':supplement_size', $supplement['supplement_size'], PDO::PARAM_INT);
			$stmt->bindValue(':supplement_hash', $supplement['supplement_hash'], PDO::PARAM_STR);
			$stmt->bindValue(':supplement_type_id', $supplement['type_id'], PDO::PARAM_INT);
			$stmt->bindValue(':supplement_mime_id', $supplement['mime_id'], PDO::PARAM_INT);
			$stmt->bindValue(':supplement_date', $this->dbDate($supplement['date']), PDO::PARAM_STR);
			$stmt->bindValue(':supplement_title', $supplement['title'], PDO::PARAM_STR);
			$stmt->bindValue(':supplement_desc', $supplement['description'], PDO::PARAM_STR);
			$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
			if (!$exists_id)
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			if ($supplement['doc'] != '')
			{
				$blob = base64_decode($supplement['doc']);
				if ($blob !== false)
				{
					$cache_file = $this->getCacheFilename('supplement', $supplement['supplement_id']);
					$this->cache->set($cache_file, $blob);
					$sql = "UPDATE ls_bill_supplement
							SET local_copy = :local_copy, local_fragment = :local_fragment
							WHERE supplement_id = :supplement_id";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':local_copy', 1, PDO::PARAM_INT);
					$stmt->bindValue(':local_fragment', $cache_file, PDO::PARAM_STR);
					$stmt->bindValue(':supplement_id', $supplement['supplement_id'], PDO::PARAM_INT);
					$stmt->execute();	
				}
				else
				{
					throw new APIException('Could not decode supplement text blob');
				}
			}

			$this->db->commit();

			$this->middlewareSignal('supplement', $supplement['supplement_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);

		} catch (APIAccessException $e) {
			$this->db->rollback();

			throw($e);

		} catch (APIException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processRollCall()
	/** 
	 * Process a roll call payload
	 *
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a roll call
	 * 
	 * @return integer[]
	 *   Returns a list of missing people_id FALSE on processing errors
	 *
	 */
	public function processRollCall($payload)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processRollCall payload status = "' . $payload['status'] . '"');

		$roll_call = $payload['roll_call'];

		try {
			if (($exists_id = $this->checkExists('bill_vote', $roll_call['roll_call_id'])))
				$sql = "UPDATE ls_bill_vote
						SET bill_id = :bill_id, roll_call_body_id = :roll_call_body_id,
							roll_call_date = :roll_call_date,
							roll_call_desc = :roll_call_desc,
							yea = :yea, nay = :nay,
							nv = :nv, absent = :absent,
							total = :total, passed = :passed,
							updated = :updated
						WHERE roll_call_id = :roll_call_id";
			else
				$sql = "INSERT INTO ls_bill_vote (
							roll_call_id, bill_id, roll_call_body_id, roll_call_date,
							roll_call_desc, yea, nay, nv, absent, total, passed,
							updated, created
						) VALUES (
							:roll_call_id, :bill_id, :roll_call_body_id, :roll_call_date,
							:roll_call_desc, :yea, :nay, :nv, :absent, :total, :passed,
							:updated, :created
						)";

			$now = date('Y-m-d H:i:s');

			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':roll_call_id', $roll_call['roll_call_id'], PDO::PARAM_INT);
			$stmt->bindValue(':bill_id', $roll_call['bill_id'], PDO::PARAM_INT);
			$stmt->bindValue(':roll_call_body_id', $roll_call['chamber_id'], PDO::PARAM_INT);
			$stmt->bindValue(':roll_call_date', $this->dbDate($roll_call['date']), PDO::PARAM_STR);
			$stmt->bindValue(':roll_call_desc', $roll_call['desc'], PDO::PARAM_STR);
			$stmt->bindValue(':yea', $roll_call['yea'], PDO::PARAM_INT);
			$stmt->bindValue(':nay', $roll_call['nay'], PDO::PARAM_INT);
			$stmt->bindValue(':nv', $roll_call['nv'], PDO::PARAM_INT);
			$stmt->bindValue(':absent', $roll_call['absent'], PDO::PARAM_INT);
			$stmt->bindValue(':total', $roll_call['total'], PDO::PARAM_INT);
			$stmt->bindValue(':passed', $roll_call['passed'], PDO::PARAM_INT);
			$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
			if (!$exists_id)
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			// Clear out old data
			if ($exists_id)
				$this->db
					->prepare('DELETE FROM ls_bill_vote_detail WHERE roll_call_id = :roll_call_id')
					->execute([$roll_call['roll_call_id']]);

			foreach ($roll_call['votes'] as $vote)
			{
				$sql = "INSERT INTO ls_bill_vote_detail (
							roll_call_id, people_id, vote_id
						) VALUES (
							:roll_call_id, :people_id, :vote_id
						)";
				$stmt = $this->db->prepare($sql);

				$stmt->bindValue(':roll_call_id', $roll_call['roll_call_id'], PDO::PARAM_INT);
				$stmt->bindValue(':people_id', $vote['people_id'], PDO::PARAM_INT);
				$stmt->bindValue(':vote_id', $vote['vote_id'], PDO::PARAM_INT);

				$stmt->execute();

				if (!$this->checkExists('people', $vote['people_id']))
					$this->request('sponsors', $vote['people_id']);
			}

			$this->db->commit();

			$this->middlewareSignal('rollcall', $roll_call['roll_call_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		}

		if (isset($this->missing['sponsors']))
			return $this->missing['sponsors'];
		else
			return array();
	}
	// }}}

	// {{{ processPerson()
	/** 
	 * Process a person payload
	 *
	 * @throws APIStatusException
	 * @throws PDOException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a person
	 * 
	 * @return boolean
	 *   Indicating the success/failure of person payload processing
	 */
	public function processPerson($payload)
	{
		$this->resetMissing(); // For consistency

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processPerson payload status = "' . $payload['status'] . '"');

		$person = $payload['person'];

		$now = date('Y-m-d H:i:s');

		try {
			if (($exists_id = $this->checkExists('people', $person['people_id'])))
				$sql = "UPDATE ls_people
						SET state_id = :state_id, role_id = :role_id,
							party_id = :party_id, name = :name,
							first_name = :first_name, middle_name = :middle_name,
							last_name = :last_name,	suffix = :suffix,
							nickname = :nickname, district = :district,
							committee_sponsor_id = :committee_sponsor_id,
							votesmart_id = :votesmart_id, followthemoney_eid = :followthemoney_eid,
							opensecrets_id = :opensecrets_id, ballotpedia = :ballotpedia,
							knowwho_pid = :knowwho_pid,	person_hash = :person_hash,
							updated = :updated
						WHERE people_id = :people_id";
			else
				$sql = "INSERT INTO ls_people (
							people_id, state_id, role_id, party_id, name,
							first_name, middle_name, last_name, suffix,
							nickname, district, committee_sponsor_id,
							votesmart_id, followthemoney_eid, opensecrets_id,
							ballotpedia, knowwho_pid, person_hash, updated, created
						) VALUES (
							:people_id, :state_id, :role_id, :party_id, :name,
							:first_name, :middle_name, :last_name, :suffix,
							:nickname, :district, :committee_sponsor_id,
							:votesmart_id, :followthemoney_eid, :opensecrets_id,
							:ballotpedia, :knowwho_pid, :person_hash, :updated, :created
						)";

			$this->db->beginTransaction();

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':people_id', $person['people_id'], PDO::PARAM_INT);
			$stmt->bindValue(':state_id', $person['state_id'], PDO::PARAM_INT);
			$stmt->bindValue(':role_id', $person['role_id'], PDO::PARAM_INT);
			$stmt->bindValue(':party_id', $person['party_id'], PDO::PARAM_INT);
			$stmt->bindValue(':name', $person['name'], PDO::PARAM_STR);
			$stmt->bindValue(':first_name', $person['first_name'], PDO::PARAM_STR);
			$stmt->bindValue(':middle_name', $person['middle_name'], PDO::PARAM_STR);
			$stmt->bindValue(':last_name', $person['last_name'], PDO::PARAM_STR);
			$stmt->bindValue(':suffix', $person['suffix'], PDO::PARAM_STR);
			$stmt->bindValue(':nickname', $person['nickname'], PDO::PARAM_STR);
			$stmt->bindValue(':district', $person['district'], PDO::PARAM_STR);
			$stmt->bindValue(':committee_sponsor_id', $person['committee_id'], PDO::PARAM_INT);
			$stmt->bindValue(':ballotpedia', $person['ballotpedia'], PDO::PARAM_STR);
			$stmt->bindValue(':followthemoney_eid', $person['ftm_eid'], PDO::PARAM_INT);
			$stmt->bindValue(':votesmart_id', $person['votesmart_id'], PDO::PARAM_INT);
			$stmt->bindValue(':knowwho_pid', $person['knowwho_pid'], PDO::PARAM_INT);
			$stmt->bindValue(':opensecrets_id', $person['opensecrets_id'], PDO::PARAM_STR);
			$stmt->bindValue(':person_hash', $person['person_hash'], PDO::PARAM_STR);
			$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
			if (!$exists_id)
				$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			$this->db->commit();

			$this->middlewareSignal('people', $person['people_id']);

		} catch (PDOException $e) {
			$this->db->rollback();

			throw($e);
		}

		return true;
	}
	// }}}

	// {{{ processSearch()
	/** 
	 * Process a search result and identify bills to be imported
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a search result
	 * 
	 * @param integer $import_type
	 *   Control what condition triggers a bill to be flagged for importing
	 *
	 * @param integer $relevance_cutoff
	 *   Results must also match have this relevance score or higher
	 * 
	 * @return integer[]
	 *   List of LegiScan bill_ids that qualify for importing
	 *
	 */
	public function processSearch($payload, $import_type = LegiScan::IMPORT_ALL, $relevance_cutoff = 0)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processSearch payload status = "' . $payload['status'] . '"');

		$results = $payload['searchresult'];

		$summary = array_shift($results);

		foreach ($results as $bill)
		{
			if ($bill['relevance'] > $relevance_cutoff)
			{
				switch ($import_type)
				{
					case LegiScan::IMPORT_NEW:
						if (!$this->checkExists('bill', $bill['bill_id']))
							$this->request('bills', $bill['bill_id']);
						break;

					case LegiScan::IMPORT_CHANGED:
					case LegiScan::IMPORT_ALL:
						$sql = "SELECT bill_id
								FROM ls_bill
								WHERE bill_id = :bill_id AND change_hash = :change_hash";
						$stmt = $this->db->prepare($sql);
						$stmt->bindValue(':bill_id', $bill_id['bill_id'], PDO::PARAM_INT);
						$stmt->bindValue(':change_hash', $bill_id['change_hash'], PDO::PARAM_STR);
						$stmt->execute();
						$exists = $stmt->fetchColumn();

						if (!$exists)
							$this->request('bills', $bill['bill_id']);
						break;
				}
			}
		}

		if (isset($this->missing['bills']))
			return $this->missing['bills'];
		else
			return array();
	}
	// }}}

	// {{{ processMonitorList()
	/** 
	 * Process a monitored list payload, adding any necessary missing child objects to 
	 * the missing list
	 *
	 * @throws APIStatusException
	 *
	 * @param array $payload
	 *   The decoded internal payload representing a monitored list
	 *
	 * @param integer $import_type 
	 *   The `LegiScan::IMPORT_*` to control how bills are selected for import
	 * 
	 * @return integer[]
	 *   Returns an array of missing/changed bill_id or FALSE on processing errors
	 */
	public function processMonitorList($payload, $import_type = LegiScan::IMPORT_ALL)
	{
		$this->resetMissing();

		// If this came from a pull, make sure status == OK
		if (isset($payload['status']) && $payload['status'] != LegiScan::API_OK)
			throw new APIStatusException('processMonitoredList payload status = "' . $payload['status'] . '"');

		foreach ($payload['monitorlist'] as $bill)
		{
			switch ($import_type)
			{
				case LegiScan::IMPORT_NEW:
					if (!$this->checkExists('bill', $bill['bill_id']))
						$this->request('bills', $bill['bill_id']);
					break;

				case LegiScan::IMPORT_CHANGED:
				case LegiScan::IMPORT_ALL:
					$sql = "SELECT bill_id
							FROM ls_bill
							WHERE bill_id = :bill_id AND change_hash = :change_hash";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
					$stmt->bindValue(':change_hash', $bill['change_hash'], PDO::PARAM_STR);
					$stmt->execute();
					$exists = $stmt->fetchColumn();

					if (!$exists)
						$this->request('bills', $bill['bill_id']);
					break;
			}
		}

		if (isset($this->missing['bills']))
			return $this->missing['bills'];
		else
			return array();
	}
	// }}}

	// {{{ monitor()
	/**
	 * Add or remove a bill_id from the `ls_monitor` list
	 *
	 * @param integer $bill_id
	 *   The bill_id to add or remove
	 *
	 * @param boolean $add
	 *   Add to list when true, remove when false
	 *
	 * @param boolean $stance
	 *   Stance on the bill from Watch, Support, Oppose
	 *
	 * @return boolean
	 *   Indicating the success/failure of manipulating ignore list
	 *
	 */
	function monitor($bill_id, $add = true, $stance = LegiScan::STANCE_WATCH)
	{
		$stmt = null;

		if ($add)
		{
			$now = date('Y-m-d H:i:s');

			if (!$this->checkExists('monitor', $bill_id))
				$sql = 'INSERT INTO ls_monitor (bill_id, stance, created) VALUES (:bill_id, :stance, :created)';
			else
				$sql = 'UPDATE ls_monitor SET stance = :stance, created = :created WHERE bill_id = :bill_id';

			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->bindValue(':stance', $stance, PDO::PARAM_INT);
			$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			// Make sure a monitored bill is not also ignored
			$sql = 'DELETE FROM ls_ignore WHERE bill_id = :bill_id';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->execute();
		}
		elseif (!$add)
		{
			$sql = 'DELETE FROM ls_monitor WHERE bill_id = :bill_id';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->execute();
		}

		return true;
	}
	// }}}

	// {{{ ignore()
	/**
	 * Add or remove a bill_id from the `ls_ignore` list
	 *
	 * @param integer $bill_id
	 *   The bill_id to add or remove
	 *
	 * @param boolean $add
	 *   Add to list when true, remove when false
	 *
	 * @return boolean
	 *   Indicating the success/failure of manipulating ignore list
	 *
	 */
	function ignore($bill_id, $add = true)
	{
		$stmt = null;

		if ($add && !$this->checkExists('ignore', $bill_id))
		{
			$now = date('Y-m-d H:i:s');
			$sql = 'INSERT INTO ls_ignore (bill_id, created) VALUES (:bill_id, :created)';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->bindValue(':created', $now, PDO::PARAM_STR);
			$stmt->execute();

			// Make sure an ignored bill is not also monitored
			$sql = 'DELETE FROM ls_monitor WHERE bill_id = :bill_id';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->execute();
		}
		elseif (!$add)
		{
			$sql = 'DELETE FROM ls_ignore WHERE bill_id = :bill_id';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
			$stmt->execute();
		}


		return true;
	}
	// }}}

	// {{{ checkExists()
	/**
	 * Check to see if a key exists in a table, results are cached via {@see LegiScan_Cache_Memory}
	 *
	 * @param string $table
	 *   Base name of the table to check
	 *
	 * @param integer $id
	 *   Key id to check for
	 *
	 * @param boolean $skip_cache
	 *   Bypass cache and do direct lookup
	 *
	 * @return boolean|integer
	 *   Returns FALSE if does not exist, otherwise the $id is returned
	 *
	 */
	public function checkExists($table, $id, $skip_cache = false)
	{
		$table = str_replace('ls_', '', strtolower($table));

		$sql = '';
		$result = false;

		$key = 'ls_' . $table . ':' . $id;

		if (($result = $this->memcache->get($key)) === false || $skip_cache)
		{
			// This could be done building a dynamic query based on the inputs, while
		    // 'safe' since only system generated inputs, but for demo verbosity...
			switch ($table)
			{
				case 'bill':
					$sql = 'SELECT bill_id FROM ls_bill WHERE bill_id = :id';
					break;
				case 'monitor':
					$sql = 'SELECT bill_id FROM ls_monitor WHERE bill_id = :id';
					break;
				case 'ignore':
					$sql = 'SELECT bill_id FROM ls_ignore WHERE bill_id = :id';
					break;
				case 'session':
					$sql = 'SELECT session_id FROM ls_session WHERE session_id = :id';
					break;
				case 'committee':
					$sql = 'SELECT committee_id FROM ls_committee WHERE committee_id = :id';
					break;
				case 'people':
					$sql = 'SELECT people_id FROM ls_people WHERE people_id = :id';
					break;
				case 'bill_text':
					$sql = 'SELECT text_id FROM ls_bill_text WHERE text_id = :id';
					break;
				case 'bill_vote':
					$sql = 'SELECT roll_call_id FROM ls_bill_vote WHERE roll_call_id = :id';
					break;
				case 'bill_amendment':
					$sql = 'SELECT amendment_id FROM ls_bill_amendment WHERE amendment_id = :id';
					break;
				case 'bill_supplement':
					$sql = 'SELECT supplement_id FROM ls_bill_supplement WHERE supplement_id = :id';
					break;
				case 'subject':
					$sql = 'SELECT subject_id FROM ls_subject WHERE subject_id = :id';
					break;
			}

			if ($sql)
			{
				$stmt = $this->db->prepare($sql);
				$stmt->bindValue(':id', $id);
				$stmt->execute();
				$result = $stmt->fetchColumn();

				if ($result)
				{
					// An annoying side effect of supporting Memcache and Memcached
					// is their incompatible set() functions, Memcached and the internal
					// LegiScan Memory Cache use the more sensible (key, value, expiration)
					if (get_class($this->memcache) == 'Memcache')
						$this->memcache->set($key, $result, false, 1800);
					else
						$this->memcache->set($key, $result, 1800);
				}
			}
		}

		return $result;
	}
	// }}}

	// {{{ makeSQLStatement()
	/**
	 * Create a PDO statatement for INSERT/UPDATE and bound values ready for execute()
	 *
	 * @param string $type
	 *   Type of SQL statement to generate 
	 *
	 * @param string $table
	 *   Table name
	 *
	 * @param array[] $updates
	 *   Array of field => array(value, type) pairs for updates
	 *
	 * @param array[] $keys
	 *   Arry of field => array(value, type) pairs for table key(s)
	 *
	 * @return PDOStatement
	 *   PDO prepared and bound statement
	 *
	 */
	private function makeSQLStatement($type, $table, $updates, $keys)
	{
		$type = strtolower($type);

		if ($type == 'update')
		{
			$sets = array();
			$sql = "UPDATE $table SET ";
			foreach (array_keys($updates) as $field)
			{
				$sets[] = "{$field} = :{$field}";
			}
			$sql .= implode(', ', $sets);

			$where = array();
			foreach (array_keys($keys) as $field)
			{
				$where[] = "{$field} = :{$field}";
			}
			$sql .= ' WHERE ' . implode(' AND ', $where);
		}
		else
		{
			$vals = array();
			$sql = "INSERT INTO $table (" . implode(', ', array_keys(array_merge($keys, $updates))) . ') VALUES (';
			foreach (array_keys(array_merge($keys, $updates)) as $field)
			{
				$vals[] = ":{$field}";
			}
			$sql .= implode(', ', $vals);
			$sql .= ')';
		}
		
		try {
			$stmt = $this->db->prepare($sql);

		} catch (PDOException $e) {
			// Catch this here to make it more verbose since EIP will be in
			// in the wrong place is there is a paramenter problem so give
			// a better hint of what when pear shaped in the throw
			throw new APIException("makeSQLStatement $type $table - " . $e->getMessage());
		}

		foreach (array_merge($updates, $keys) as $field => $value)
		{
			$stmt->bindValue(":{$field}", $value[0], $value[1]);
		}

		return $stmt;
	}
	// }}}

	// {{{ billInit()
	/**
	 * Create a skeleton data structure that will be filled by {@see loadBill()}
	 *
	 * @return mixed[]
	 *   The blank bill data structure
	 *
	 */
	private function billInit()
	{
		// Empty template bill array

		$bill = array();

		$bill['bill_id'] = 0;
		$bill['change_hash'] = '';
		$bill['session'] = array('prefile'=>0,'sine_die'=>0,'prior'=>0);
		$bill['session_id'] = 0;
		$bill['url'] = '';
		$bill['state_link'] = '';
		$bill['completed'] = 0; // DEPRECATED DO NOT USE
		$bill['status'] = 0;
		$bill['status_date'] = '';
		$bill['progress'] = array();
		$bill['state'] = '';
		$bill['state_id'] = 0;
		$bill['bill_number'] = '';
		$bill['bill_type'] = '';
		$bill['bill_type_id'] = 0;
		$bill['body'] = '';
		$bill['body_id'] = 0;
		$bill['current_body'] = '';
		$bill['current_body_id'] = 0;
		$bill['title'] = '';
		$bill['description'] = '';
		$bill['pending_committee_id'] = 0;
		$bill['committee'] = array();
		$bill['referrals'] = array();
		$bill['history'] = array();
		$bill['sponsors'] = array();
		$bill['sasts'] = array();
		$bill['subjects'] = array();
		$bill['texts'] = array();
		$bill['votes'] = array();
		$bill['amendments'] = array();
		$bill['supplements'] = array();
		$bill['calendar'] = array();

		return $bill;
	}
	// }}}

	// {{{ loadBill()
	/**
	 * Recreate a data structure that mimics the getBill payload for comparison when updating existing bills
	 *
	 * @param integer $bill_id
	 *   The bill_id to load
	 * 
	 * @return mixed[]
	 *   The data structure representing a bill
	 *
	 */
	public function loadBill($bill_id)
	{
		$bill = $this->billInit();

		$sql = 'SELECT *
				FROM ls_bill b
				INNER JOIN ls_session s ON b.session_id = s.session_id
				WHERE b.bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		$r = $stmt->fetch();

		if (empty($r))
			return $bill;

		$bill['bill_id'] = $r['bill_id'];
		$bill['change_hash'] = $r['change_hash'];
		$bill['session_id'] = $r['session_id'];
		$bill['session'] = array(
			'session_id' => $r['session_id'],
			'year_start' => $r['year_start'],
			'year_end' => $r['year_end'],
			'prefile' => $r['prefile'],
			'sine_die' => $r['sine_die'],
			'prior' => $r['prior'],
			'special' => $r['special'],
			'session_tag' => $r['session_tag'],
			'session_title' => $r['session_title'],
			'session_name' => $r['session_name'],
		);
		$bill['url'] = $r['legiscan_url'];
		$bill['state_link'] = $r['state_url'];
		$bill['status'] = $r['status_id'];
		$bill['status_date'] = $r['status_date'];
		$bill['state_id'] = $r['state_id'];
		$bill['bill_number'] = $r['bill_number'];
		$bill['bill_type_id'] = $r['bill_type_id'];
		$bill['body_id'] = $r['body_id'];
		$bill['current_body_id'] = $r['current_body_id'];
		$bill['title'] = $r['title'];
		$bill['description'] = $r['description'];
		$bill['pending_committee_id'] = $r['pending_committee_id'];
		if ($r['pending_committee_id'])
		{
			$sql = 'SELECT * FROM ls_committee WHERE committee_id = :pending_committee_id';
			$stmt = $this->db->prepare($sql);
			$stmt->bindValue(':pending_committee_id', $r['pending_committee_id'], PDO::PARAM_INT);
			$stmt->execute();
			$r = $stmt->fetch();
			$bill['committee'] = array(
				'committee_id' => $r['committee_id'],
				'chamber_id' => $r['committee_body_id'],
				'name' => $r['committee_name']
			);
		}

		$sql = 'SELECT * FROM ls_bill_referral WHERE bill_id = :bill_id ORDER BY referral_step';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['referrals'][] = array(
				'date' => $r['referral_date'],
				'committee_id' => $r['committee_id'],
			);
		}

		$sql = 'SELECT * FROM ls_bill_history WHERE bill_id = :bill_id ORDER BY history_step';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['history'][] = array(
				'date' => $r['history_date'],
				'action' => $r['history_action'],
				'chamber_id' => $r['history_body_id'],
				'importance' => $r['history_major']
			);
		}

		$sql = 'SELECT * FROM ls_bill_progress WHERE bill_id = :bill_id ORDER BY progress_step';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['progress'][] = array(
				'date' => $r['progress_date'],
				'event' => $r['progress_event_id']
			);
		}

		$sql = 'SELECT *
				FROM ls_bill_sponsor bs
					INNER JOIN ls_people p ON bs.people_id = p.people_id
				WHERE bill_id = :bill_id ORDER BY bs.sponsor_order';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['sponsors'][] = array(
				'people_id' => $r['people_id'],
				'person_hash' => $r['person_hash'],
				'party_id' => $r['party_id'],
				'role_id' => $r['role_id'],
				'name' => $r['name'],
				'first_name' => $r['first_name'],
				'middle_name' => $r['middle_name'],
				'last_name' => $r['last_name'],
				'suffix' => $r['suffix'],
				'nickname' => $r['nickname'],
				'district' => $r['district'],
				'ftm_eid' => $r['followthemoney_eid'],
				'votesmart_id' => $r['votesmart_id'],
				'opensecrets_id' => $r['opensecrets_id'],
				'ballotpedia' => $r['ballotpedia'],
				'sponsor_type_id' => $r['sponsor_type_id'],
				'sponsor_order' => $r['sponsor_order'],
				'committee_id' => $r['committee_sponsor_id']
			);
		}

		$sql = 'SELECT * FROM ls_bill_vote WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['votes'][] = array(
				'roll_call_id' => $r['roll_call_id'],
				'date' => $r['roll_call_date'],
				'desc' => $r['roll_call_desc'],
				'yea' => $r['yea'],
				'nay' => $r['nay'],
				'nv' => $r['nv'],
				'absent' => $r['absent'],
				'total' => $r['total'],
				'passed' => $r['passed'],
				'chamber_id' => $r['roll_call_body_id'],
				'url' => $r['legiscan_url'],
				'state_link' => $r['state_url']	
			);
		}

		$sql = 'SELECT * FROM ls_bill_text WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['texts'][] = array(
				'doc_id' => $r['text_id'],
				'date' => $r['bill_text_date'],
				'type_id' => $r['bill_text_type_id'],
				'mime_id' => $r['bill_text_mime_id'],
				'url' => $r['legiscan_url'],
				'state_link' => $r['state_url'],
				'text_size' => $r['bill_text_size'],
				'text_hash' => $r['bill_text_hash']
			);
		}

		$sql = 'SELECT * FROM ls_bill_amendment WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['amendments'][] = array(
				'amendment_id' => $r['amendment_id'],
				'adopted' => $r['adopted'],
				'chamber_id' => $r['amendment_body_id'],
				'date' => $r['amendment_date'],
				'title' => $r['amendment_title'],
				'description' => $r['amendment_desc'],
				'mime_id' => $r['amendment_mime_id'],
				'url' => $r['legiscan_url'],
				'state_link' => $r['state_url'],
				'amendment_size' => $r['amendment_size'],
				'amendment_hash' => $r['amendment_hash']
			);
		}

		$sql = 'SELECT * FROM ls_bill_supplement WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['supplements'][] = array(
				'supplement_id' => $r['supplement_id'],
				'date' => $r['supplement_date'],
				'type_id' => $r['supplement_type_id'],
				'title' => $r['supplement_title'],
				'description' => $r['supplement_desc'],
				'mime_id' => $r['supplement_mime_id'],
				'url' => $r['legiscan_url'],
				'state_link' => $r['state_url'],
				'supplement_size' => $r['supplement_size'],
				'supplement_hash' => $r['supplement_hash']
			);
		}

		$sql = 'SELECT * FROM ls_bill_sast WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['sasts'][] = array(
				'sast_bill_id' => $r['sast_bill_id'],
				'sast_type_id' => $r['sast_type_id']
			);
		}

		$sql = 'SELECT bs.subject_id, s.subject_name
				FROM ls_bill_subject bs
					INNER JOIN ls_subject s ON bs.subject_id = s.subject_id
				WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['subjects'][] = array(
				'subject_id' => $r['subject_id'],
				'subject_name' => $r['subject_name']
			);
		}

		$sql = 'SELECT * FROM ls_bill_calendar WHERE bill_id = :bill_id';
		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':bill_id', $bill_id, PDO::PARAM_INT);
		$stmt->execute();
		while ($r = $stmt->fetch())
		{
			$bill['calendar'][] = array(
				'event_hash' => $r['event_hash'],
				'type_id' => $r['event_type_id'],
				'date' => $r['event_date'],
				'time' => $r['event_time'],
				'location' => $r['event_location'],
				'description' => $r['event_desc']
			);
		}

		return $bill;
	}
	// }}}

	// {{{ request()
	/**
	 * Add an `object:id` pair to the missing list while preventing duplications
	 *
	 * @param string $object
	 *   The name of the type of object being requested
	 *
	 * @param integer $id
	 *   The object LegiScan ID of the requested object
	 * 
	 * @return boolean
	 *   Indicating the success/failure of adding to the missing list
	 *
	 */
	public function request($object, $id)
	{
		// Hobo cache
		static $requested = array();

		$key = $object . ':' . $id;

		if (!isset($requested[$key]))
		{
			$this->missing[$object][] = $id;
			$requested[$key] = 1;
		}

		return true;
	}
	// }}}

	// {{{ getMissing()
	/** 
	 * Get the list of missing objects, if any, from the most recent process run
	 *
	 * @return array
	 *   The structured list of missing objects
	 */
	public function getMissing()
	{
		return $this->missing;
	}
	// }}}

	// {{{ resetMissing()
	/** 
	 * Reset the list of missing objects
	 *
	 * @return boolean
	 *   Indicating success/failure of the reset
	 *
	 */
	public function resetMissing()
	{
		$this->missing = array();

		return true;
	}
	// }}}

	// {{{ getDB()
	/**
	 * Return the DB handle so others can play if needed
	 *
	 * @return PDO
	 *   Database PDO handle
	 *
	 */
	public function getDB()
	{
		return $this->db;
	}
	// }}}

	// {{{ dbDate()
	/**
	 * Massage '0000-00-00' date to NULL when operating under PostgreSQL
	 *
	 * @param string $date
	 *   The date to massage
	 *
	 * @return string|null
	 *   The massaged and relaxed date
	 *
	 */
	private function dbDate($date)
	{
		if ($this->massage && ($date == '0000-00-00' || $date == ''))
			$date = null;

		return $date;
	}
	// }}}

	// {{{ getStateList()
	/**
	 * Get a list of state table records for ALL substitutions
	 *
	 * @return array[]
	 *   Array of state table rows
	 */
	public function getStateList()
	{
		static $state_list = array();

		if (empty($state_list))
		{
			$sql = 'SELECT * FROM ls_state ORDER BY state_abbr';
			$rs = $this->db->query($sql);
			while ($r = $rs->fetch())
			{
				$state_list[$r['state_id']] = $r;
			}
		}

		return $state_list;
	}
	// }}}

	// {{{ middlewareSignal()
	/**
	 * Send a signal to the middleware that data has changed so appropriate
	 * steps can be taken
	 *
	 * @param string $object
	 *   The type of object
	 *
	 * @param integer $id
	 *   The object ID
	 * 
	 * @return boolean
	 *   Indicating the success/failure of sending signal
	 *
	 */
	public function middlewareSignal($object, $id)
	{
		$signal = strtolower(LegiScan::getConfig('middleware_signal'));

		if (!$signal)
			return true;

		$now = date('Y-m-d H:i:s', time());
		$object = strtolower($object);

		if ($signal == 'table')
		{
			try {
				$sql = "SELECT * FROM ls_signal WHERE object_type = :object AND object_id = :id";
				$stmt = $this->db->prepare($sql);
				$stmt->bindValue(':object', $object, PDO::PARAM_STR);
				$stmt->bindValue(':id', $id, PDO::PARAM_INT);
				$stmt->execute();
				$semaphore = $stmt->fetch();

				if ($semaphore == false)
					$sql = 'INSERT INTO ls_signal (
								object_type, object_id, processed,
								updated, created
							) VALUES (
								:object_type, :object_id, :processed,
								:updated, :created
							)';
				else
					$sql = 'UPDATE ls_signal
							SET processed = :processed, updated = :updated
							WHERE object_type = :object_type AND object_id = :object_id';

				// Only for new entries and those that have been processed,
				// this preserves the original timestamp in case a second
				// update comes in before middleware processing
				if (!$semaphore || isset($semaphore['processed']) && $semaphore['processed'] >= 1)
				{
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':object_type', $object, PDO::PARAM_STR);
					$stmt->bindValue(':object_id', $id, PDO::PARAM_INT);
					$stmt->bindValue(':processed', 0, PDO::PARAM_INT);
					$stmt->bindValue(':updated', $now, PDO::PARAM_STR);
					if (!$semaphore)
						$stmt->bindValue(':created', $now, PDO::PARAM_STR);
					$stmt->execute();
				}

			} catch (PDOException $e) {
				return false;
			}
		}
		elseif ($signal == 'directory')
		{
			$filename = $object . '.' . $id;
			$file = realpath(__DIR__ . '/' . 'signal') . '/' . $filename;

			$data = json_encode(array(
				'object_type' => (string) $object,
				'object_id' => (int) $id,
				'updated' => (string) $now,
			));

			// If the second update comes in before middleware processes
			// leave the contents to preserve the original timestamp
			if (!file_exists($file))
			{
				if (!@file_put_contents($file, $data))
					return false;
			}
		}

		return true;
	}
	// }}}

	// {{{ getCacheFilename()
	/**
	 * Generate a document cache filename path fragment
	 *
	 * @param string $object
	 *   The type of document object
	 *
	 * @param integer $id
	 *   The document object ID
	 * 
	 * @return string
	 *   Filename path fragment for the location under the document cache directory
	 *
	 */
	public function getCacheFilename($object, $id)
	{
		// This is a mapping of mime_type_id to extension
		$suffix = array(
			1 => 'html',
			2 => 'pdf',
			3 => 'wpd',
			4 => 'doc',
			5 => 'rtf',
			6 => 'docx',
		);

		$object = strtolower($object);

		switch ($object)
		{
			case 'text':
				$sql = 'SELECT s.state_abbr, b.bill_number, b.session_id, bt.bill_text_mime_id AS mime_id
						FROM ls_bill_text bt
							INNER JOIN ls_bill b ON bt.bill_id = b.bill_id
							INNER JOIN ls_state s ON b.state_id = s.state_id
						WHERE bt.text_id = :id';
				break;
			case 'amendment':
				$sql = 'SELECT s.state_abbr, b.bill_number, b.session_id, ba.amendment_mime_id AS mime_id
						FROM ls_bill_amendment ba
							INNER JOIN ls_bill b ON ba.bill_id = b.bill_id
							INNER JOIN ls_state s ON b.state_id = s.state_id
						WHERE ba.amendment_id = :id';
				break;
			case 'supplement':
				$sql = 'SELECT s.state_abbr, b.bill_number, b.session_id, bs.supplement_mime_id AS mime_id
						FROM ls_bill_supplement bs
							INNER JOIN ls_bill b ON bs.bill_id = b.bill_id
							INNER JOIN ls_state s ON b.state_id = s.state_id
						WHERE bs.supplement_id = :id';
				break;
		}

		$stmt = $this->db->prepare($sql);
		$stmt->bindValue(':id', $id, PDO::PARAM_INT);
		$stmt->execute();
		$r = $stmt->fetch();

		// Custom storage layout? Make the change here and the system 
		// will take care of creating any needed subdirectories. Though
		// it is an exercise for the reader to relocate any existing
		// files in the standard location
		
		// Build the actual filename
		$filename = $object . '/' . $r['state_abbr'] . '/' . $r['session_id'] . '/' . $r['bill_number'] . '/' . $id . '/' . $suffix[$r['mime_id']];
		$filename = strtolower($filename);

		return $filename;
	}
	// }}}
}
// }}}
