<?php
/**
 * Common functions and such
 *
 * @package LegiScan\Common
 */

global $legiscan_options;

// {{{ legiscan_getopt()
/**
 * Wrapper around getopt() that also strips recognized options from $argv
 *
 * @param array $valid_options
 *   An array of long options that are valid
 *
 * @param string $extra
 *   Additional text that will be added to --help output
 *
 * @return array
 *   The parsed options and command arguments
 *
 */
function legiscan_getopt($valid_options = array(), $extra = '')
{
	global $argv, $argc, $legiscan_options;

	if (!empty($valid_options))
	{
		$parameters['short'] = array(
			'h',
		);
		$parameters['long'] = array_merge($valid_options, array('help','version'));
	}

	$opt_short = implode('', array_values($parameters['short']));
	$opt_long = array_values($parameters['long']);
	$legiscan_options = getopt($opt_short, $opt_long);

	if (isset($legiscan_options['h']) || isset($legiscan_options['help']) || $legiscan_options === false)
	{
		legiscan_help($parameters['long'], $extra);
	}

	if (isset($legiscan_options['version']))
	{
		echo $argv[0] . ' ' . LegiScan::VERSION . "\n";
		exit(0);
	}

	// Clean $argv of recognized options
	$pruneargv = array();
	foreach ($legiscan_options as $option => $value)
	{
		// Force non-parameter options to be true in the options array
		if (!$value)
			$legiscan_options[$option] = true;
		foreach ($argv as $key => $chunk)
		{
			$regex = '/^'. (isset($option[1]) ? '--' : '-') . $option . '/';
			if ($chunk == $value && $argv[$key-1][0] == '-' || preg_match($regex, $chunk))
			{
				array_push($pruneargv, $key);
				$argc--;
			}
		}
	}
	while ($key = array_pop($pruneargv))
		unset($argv[$key]);

	$argv = array_values($argv);

	return $legiscan_options;
}
// }}}
// {{{ legiscan_option()
/**
 * Check to see if an option is set and return the value if it had an argument, otherwise true/false
 *
 * @param string $option
 *   Option name to lookup
 *
 * @param mixed $default
 *   Return $default if no option set
 *
 * @return mixed
 *   The parameter argument or true if a simple option set
 *
 */
function legiscan_option($option, $default = false)
{
	global $legiscan_options;

	if (isset($legiscan_options[$option]))
		return $legiscan_options[$option];
	else
		return $default;
}
// }}}
// {{{ legiscan_help()
/**
 * Print some help text for command line options and exit()
 *
 * @param array $parameters
 *   The valid list of command line options
 *
 * @param string $extra
 *   Additional text that will be added to output
 *
 */
function legiscan_help($parameters, $extra = '')
{
	global $argv;

	$help = array(
		"monitoredlist"		=> "--monitoredlist     \tRetrieve monitored list for GAITS account",
		"sessionlist:"		=> "--sessionlist STATE \tRetrieve session list info for STATE or ALL",
		"masterlist:"		=> "--masterlist STATE  \tRetrieve master list for STATE",
		"bill:"				=> "--bill BILL_ID      \tRetrieve bill detail payload for BILL_ID",
		"text:"				=> "--text DOC_ID       \tRetrieve text payload for DOC_ID",
		"amendment:"		=> "--amendment AMEND_ID\tRetrieve amendment payload for AMEND_ID",
		"supplement:"		=> "--supplement SUP_ID \tRetrieve supplement payload for SUP_ID",
		"vote:"				=> "--vote ROLL_CALL_ID \tRetrieve roll call detail payload for ROLL_CALL_ID",
		"people:"			=> "--people PEOPLE_ID  \tRetrieve people detail payload for PEOPLE_ID",
		"search:"			=> "--search QUERY      \tRetrieve search results for QUERY",
		"state:"			=> "--state STATE       \tSet STATE for query, defaults to all states",
		"score:"			=> "--score RELEVANCE   \tSet RELEVANCE score cutoff (0-100) defaults to 50",
		"import:"			=> "--import ACTION     \tTake ACTION on search/masterlist results: New, Changed, All",
		"key:"				=> "--key API_KEY       \tOverride legiscan.conf api_key with API_KEY to use for request",
		"sync"				=> "--sync              \tSync local monitor/ignore operations to GAITS account",
		"monitor:"			=> "--monitor BILL_ID   \tAdd BILL_ID to ls_monitor list",
		"unmonitor:"		=> "--unmonitor BILL_ID \tRemove BILL_ID from ls_monitor list",
		"ignore:"			=> "--ignore BILL_ID    \tAdd BILL_ID to ls_ignore list",
		"unignore:"			=> "--unignore BILL_ID  \tRemove BILL_ID from ls_ignore list",
		"stance:"			=> "--stance STANCE     \tStance of monitored bill (0=watch,1=support,2=oppose)", 
		"clean"				=> "--clean             \tClean the API cache of stale records",
		"bulk"				=> "--bulk              \tRun a bulk update based on config.php settings",
		"scan"				=> "--scan              \tScan the available dataset archives",
		"import"			=> "--import            \tImport selected bulk dataset archives",
		"year:"				=> "--year YEAR         \tSet YEAR for filtering results",
		"session:"			=> "--session SESSION_ID\tFilter for specific SESSION_IDs",
		"skip:"				=> "--skip SESSION_ID   \tFilter against specific SESSION_IDs",
		"file:"				=> "--file ZIP          \tDataset zip archive to process",
		"regular"           => "--regular           \tFlag only regular session datasets",
		"special"           => "--special           \tFlag only special session datasets",
		"reset-db"			=> "--reset-db          \tTruncate and reset database tables back to clean production install",
		"dry-run"			=> "--dry-run           \tDon't take action only print out what would have been done",
		"yes"				=> "--yes               \tAutomatically answer Yes to any Yes/No prompts",
		"verbose"			=> "--verbose           \tBe verbose in general",
		"debug"				=> "--debug             \tBe more verbose specifically, maybe dump some JSON payload",
		"version"			=> "--version           \tShow the application version number",
		"daemon"			=> "--daemon            \tRun infinitely (otherwise update and exit)",
		"help"				=> "--help              \tDisplay this help text",
	);

	echo "Usage: " . basename($argv[0]) . " [OPTIONS]\n\n";
	if ($extra)
		echo trim($extra) . "\n\n";

	foreach ($parameters as $opt)
	{
		if (isset($help[$opt]))
			echo "  {$help[$opt]}\n";
		else
			echo sprintf("  --%-16s\t ***** NO HELP AVAILABLE *****\n", $opt);
	}

	exit(0);
}
// }}}

// {{{ expect_user()
/**
 * Check to see if the current user is the expected user
 *
 * @param string $expect
 *   The user name that is expected
 *
 * @return boolean
 *   True if running as expected user, false otherwise
 *
 */
function expect_user($expect)
{
	$user = posix_getpwuid(posix_geteuid());
	$whoami = $user['name'];

	if (strcasecmp($expect, $whoami) == 0)
		return true;
	else
		return false;
}
// }}}

// {{{ yes_no_prompt()
/**
 * Ask for a Yes/No response if on terminal, otherwise return default answer
 *
 * @param boolean $default
 *   The answer used if not interactive
 *
 * @return boolean
 *   True if the answer is Yes, false if the answer is No
 *
 */
function yes_no_prompt($default = false)
{
	$answer = $default;
	if (is_cli())
	{
		echo 'Are you sure you want to do this? (y/N) ';
		$handle = fopen('php://stdin','r');
		$line = fgets($handle);
		fclose($handle);
		$response = substr(strtolower(trim($line)), 0, 1);
		if ($response == 'y')
			$answer = true;
		else
			$answer = false;
	}

	return $answer;
}
// }}}

// {{{ is_cli()
/**
 * Simple check to see if script running via the CLI SAPI
 *
 * @return boolean
 *   True if running under CLI, false otherwise
 *
 */
function is_cli()
{
	static $is_cli;

	if (!isset($is_cli))
	{
		if (php_sapi_name() == "cli")
			$is_cli = true;
		else
			$is_cli = false;
	}

	return $is_cli;
}
// }}}
// {{{ memory_usage()
/**
 * Get the current and peak memory usage as a string for logging
 *
 * @return string
 *   The memory usage log message
 *
 */
function memory_usage()
{
	$m1 = number_format(memory_get_usage() / 1048576, 1) . 'MB';
	$m2 = number_format(memory_get_usage(true) / 1048576, 1) . 'MB';
	$mp1 = number_format(memory_get_peak_usage() / 1048576, 1) . 'MB';
	$mp2 = number_format(memory_get_peak_usage(true) / 1048576, 1) . 'MB';

	$str = "Memory: $m1 (Real: $m2) -- Peak: $mp1 (Real: $mp2)";

	return $str;
}
// }}}
// {{{ sec2hms()
/**
 * Convert elapsed seconds to H:M:S format
 *
 * @param integer $num_secs
 *   The elapsed number of seconds to convert
 *
 * @return string
 *   The H:M:S string for $num_secs
 *
 */
function sec2hms($num_secs) {
	$str = '';

	$hours = intval(intval($num_secs) / 3600);
	$str .= sprintf('%02d', $hours) . ':';

	$minutes = intval(((intval($num_secs) / 60) % 60));
	if ($minutes < 10) $str .= '0';
		$str .= $minutes.':';

	$seconds = intval(intval(($num_secs % 60)));
	if ($seconds < 10) $str .= '0';
		$str .= $seconds;

	return($str);
}
// }}}

// {{{ prettyPrint()
/**
 * Given a compact JSON string return a pretty printed version
 *
 * @param string $json
 *   The JSON string to be made fabulous
 *
 * @return string
 *   The beautified JSON string
 *
 */
function prettyPrint($json)
{
	$indent_char = '  ';
	$result = '';
	$level = 0;
	$prev_char = '';
	$in_quotes = false;
	$ends_line_level = null;
	$json_length = strlen($json);

	for ($i = 0; $i < $json_length; $i++)
	{
		$char = $json[$i];
		$new_line_level = null;
		$post = '';
		if ($ends_line_level !== null)
		{
			$new_line_level = $ends_line_level;
			$ends_line_level = null;
		}

		if ($char === '"' && $prev_char != "\\")
		{
			$in_quotes = !$in_quotes;
		}
		elseif (!$in_quotes)
		{
			switch ($char)
			{
				case '}': case ']':
					$level--;
					$ends_line_level = null;
					$new_line_level = $level;
					break;

				case '{': case '[':
					$level++;
				case ',':
					$ends_line_level = $level;
					break;

				case ':':
					$post = ' ';
					break;

				case ' ': case "\t": case "\n": case "\r":
					$char = '';
					$ends_line_level = $new_line_level;
					$new_line_level = null;
					break;
			}
		}

		if ($new_line_level !== null)
		{
			$result .= "\n" . str_repeat($indent_char, $new_line_level);
		}

		$result .= $char . $post;
		$prev_char = $char;
	}

	return $result;
}
// }}}

// {{{ timer_clear()
/**
 * Clear and reset a named timer
 *
 * @param string $name
 *   The name of the timer
 *
 */
function timer_clear($name)
{
	global $timers;

	$timers[$name] = array();
}
// }}}
// {{{ timer_read()
/**
 * Read a named timer value without stopping it
 *
 * @param string $name
 *   The name of the timer
 *
 * @return float
 *   The current timer value
 *
 */
function timer_read($name)
{
	global $timers;

	$stop = microtime($true);
	$diff = round(($stop - $timers[$name]['start']), 2);

	return $timers[$name]['time'] + $diff;
}
// }}}
// {{{ timer_start()
/**
 * Start a named timer, if a timer is started and stopped multiple times, the intervals will be accumulated
 *
 * @param string $name
 *   The name of the timer
 *
 */
function timer_start($name)
{
	global $timers;

	$timers[$name]['start'] = microtime(true);
	$timers[$name]['count'] = isset($timers[$name]['count']) ? ++$timers[$name]['count'] : 1;
}
// }}}
// {{{ timer_stop()
/**
 * Stop a named timer and return the timer values
 *
 * @param string $name
 *   The name of the timer
 *
 * @return array
 *   Contains the count of timer start/stops and the accumulated time in ms
 *
 */
function timer_stop($name)
{
	global $timers;

	$stop = microtime(true);
	$diff = round(($stop - $timers[$name]['start']) , 2);

	$timers[$name]['time'] += $diff;

	return $timers[$name];
}
// }}}
