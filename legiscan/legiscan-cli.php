<?php
/**
 * A command line interface for the LegiScan API classes
 *
 * @package LegiScan\Utility
 * @author LegiScan API Team <api@legiscan.com>
 * @license https://opensource.org/licenses/BSD-2-Clause
 * @copyright 2010-2020 LegiScan LLC
 *
 */

// I say thee nay!
if (version_compare(PHP_VERSION, '5.4.0') < 0)
	die('PHP 5.4.0 or higher is required');

// Include the LegiScan API Client
require_once('LegiScan.php');

// {{{ LegiScan_CommandLine Class
// Wrapper class to facilitate code documenter
/**
 * A command line utility for pulls and system internals
 *
 * The CLI interface allow for importing specific objects from the command line
 * along with manipulating some internal controls. This could be used for
 * testing purposes or for automation with other custom scripting.
 *
 * Show the command help
 *
 * <code>php legiscan-cli.php --help</code>
 *
 * Import the current GAITS monitoring list associated with the API key and sync
 * with the local `ls_monitor` list
 *
 * <code>php legiscan-cli.php --monitorlist --import all --sync</code>
 *
 * Get the master list for the most recent session in California, but do not
 * actually import and dump the response payload
 *
 * <code>php legiscan-cli.php --dry-run --debug --masterlist CA</code>
 *
 * Request a specific `bill_id`
 *
 * <code>php legiscan-cli.php --bill 823201</code>
 *
 * Run a national search and import new bills with a relevance score of 75% or higher
 *
 * <code>php legiscan-cli.php --search "citizens ADJ united" --state ALL --import new --score 75</code>
 *
 * Add several `bill_id` to the monitor list with support stance and synchronize with GAITS
 *
 * <code>php legiscan-cli.php --monitor 843038,908829,817390 --stance support --sync</code>
 *
 * Add a `bill_id` to the ignore list
 *
 * <code>php legiscan-cli.php --ignore 898381</code>
 *
 * Clean the API cache of stale entries
 *
 * <code>php legiscan-cli.php --clean --verbose</code>
 *
 * @see LegiScan_Process
 * @see LegiScan_Pull
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_CommandLine
{
	// {{{ commands()
	/**
	 * Make appropriate API calls and processes results into the database
	 *
	 */
	function commands()
	{
		$debug = legiscan_option('debug');
		$verbose = legiscan_option('verbose');
		$dry_run = legiscan_option('dry-run');

		$resp = array();

		try {
			// Create an instance to generate pull requests
			$legiscan = new LegiScan_Pull();

			// Create an instance to write to database
			$logic = new LegiScan_Process();

			// Determine which particular API call or action needs done
			if (legiscan_option('monitorlist'))
			{
				$filter = array('record'=>'current');
				$resp = $legiscan->getMonitorListRaw($filter);
				if ($resp)
				{
					$import = Legiscan::IMPORT_NONE;
					if (legiscan_option('import'))
					{
						switch (strtolower(legiscan_option('import')))
						{
							case 'new':
							case 1:
								$import = Legiscan::IMPORT_NEW;
								break;
							case 'changed':
							case 2:
								$import = Legiscan::IMPORT_CHANGED;
								break;
							case 'all':
							case 3:
								$import = Legiscan::IMPORT_ALL;
								break;
						}
					}

					// Optionally sync local and GAITS monitoring lists
					if (legiscan_option('sync'))
					{
						foreach ($resp['monitorlist'] as $item)
						{
							$logic->monitor($item['bill_id'], true, $item['stance']);
						}
					}

					$bill_list = $logic->processMonitorList($resp, $import);
					if ($bill_list)
					{
						if ($verbose) echo "MonitorList found " . count($bill_list) . " new bills\n";
						if (!$dry_run) $legiscan->importBillList($bill_list, $logic);
					}
				}
			}
			elseif (($session_list = legiscan_option('sessionlist')))
			{
				$list = array($session_list);
				if (strtolower($session_list) == 'all')
				{
					$list = array();
					$state_list = $logic->getStateList();
					foreach ($state_list as $state)
						$list[] = $state['state_abbr'];
				}

				foreach ($list as $state)
				{
					if ($verbose) echo "Getting session list for $state\n";
					$resp = $legiscan->getSessionList($state);
					if ($resp && !$dry_run)
						$logic->processSessionList($resp);
				}
			}
			elseif (($master_list = legiscan_option('masterlist')))
			{
				$resp = $legiscan->getMasterListRaw($master_list);
				if ($resp)
				{
					$import = Legiscan::IMPORT_NONE;
					if (legiscan_option('import'))
					{
						switch (strtolower(legiscan_option('import')))
						{
							case 'new':
							case 1:
								$import = Legiscan::IMPORT_NEW;
								break;
							case 'changed':
							case 2:
								$import = Legiscan::IMPORT_CHANGED;
								break;
							case 'all':
							case 3:
								$import = Legiscan::IMPORT_ALL;
								break;
						}
					}

					$bill_list = $logic->processMasterList($resp, $import);
					if ($bill_list)
					{
						if ($verbose) echo "MasterList found " . count($bill_list) . " new bills\n";
						if (!$dry_run) $legiscan->importBillList($bill_list, $logic);
					}
				}
			}
			elseif (($bill = legiscan_option('bill')))
			{
				// Cheat a little as importBillList() will import all other
				// related and requested missing objects (text, votes, etc)
				// so build a "bill_list" of one
				$bill_list = array($bill);

				// Prime $resp in case we are going to debug dump later
				$resp = $legiscan->getBill($bill);
				if ($resp && !$dry_run)
					$legiscan->importBillList($bill_list, $logic);
			}
			elseif (($text = legiscan_option('text')))
			{
				$resp = $legiscan->getBillText($text);
				if ($resp && !$dry_run)
					$logic->processBillText($resp);
			}
			elseif (($amendment = legiscan_option('amendment')))
			{
				$resp = $legiscan->getAmendment($amendment);
				if ($resp && !$dry_run)
					$logic->processAmendment($resp);
			}
			elseif (($supplement = legiscan_option('supplement')))
			{
				$resp = $legiscan->getSupplement($supplement);
				if ($resp && !$dry_run)
					$logic->processSupplement($resp);
			}
			elseif (($vote = legiscan_option('vote')))
			{
				$resp = $legiscan->getRollCall($vote);
				if ($resp && !$dry_run)
				{
					$people_list = $logic->processRollCall($resp);
					foreach ($people_list as $people_id)
					{
						$person = $legiscan->getPerson($people_id);
						if ($person)
							$logic->processPerson($person);
					}
				}
			}
			elseif (($people = legiscan_option('people')))
			{
				$resp = $legiscan->getPerson($people);
				if ($resp && !$dry_run)
					$logic->processPerson($resp);
			}
			elseif (($query = legiscan_option('search')))
			{
				$search = array();
				$search['query'] = $query;
				$state = strtoupper(legiscan_option('state'));
				if ($state)
					$search['state'] = $state;
				$resp = $legiscan->getSearchRaw($search);
				if ($resp)
				{
					// Start default at 50
					$score = 50;
					// Check legiscan.conf
					if (($default_rel = LegiScan::getConfig('relevance')))
						$score = $default_rel;
					// Allow command line override
					if (legiscan_option('score'))
						$score = legiscan_option('score');

					$import = Legiscan::IMPORT_NONE;
					if (legiscan_option('import'))
					{
						switch (strtolower(legiscan_option('import')))
						{
							case 'new':
							case 1:
								$import = Legiscan::IMPORT_NEW;
								break;
							case 'changed':
							case 2:
								$import = Legiscan::IMPORT_CHANGED;
								break;
							case 'all':
							case 3:
								$import = Legiscan::IMPORT_ALL;
								break;
						}
					}

					$bill_list = $logic->processSearch($resp, $import, $score);
					if ($bill_list)
					{
						if ($verbose) echo "Search found " . count($bill_list) . " new bills\n";
						if (!$dry_run) $legiscan->importBillList($bill_list, $logic);
					}
				}
			}
			elseif (legiscan_option('monitor'))
			{
				$stance = strtolower(legiscan_option('stance'));
				if ($stance == 1 || $stance == 'support')
					$stance = LegiScan::STANCE_SUPPORT;
				elseif ($stance == 2 || $stance == 'oppose')
					$stance = LegiScan::STANCE_OPPOSE;
				else
					$stance = LegiScan::STANCE_WATCH;

				$list = array();
				foreach (explode(',', legiscan_option('monitor')) as $bill_id)
				{
					$bill_id = (int) $bill_id;
					if ($bill_id)
					{
						$logic->monitor($bill_id, true, $stance);
						$list[] = $bill_id;
					}
				}
				if (legiscan_option('sync') && !empty($list))
				{
					$resp = $legiscan->setMonitor(array('list'=>$list,'action'=>'monitor','stance'=>$stance));
					foreach ($resp['return'] as $bill_id => $msg)
						if ($verbose || stripos($msg, 'ERROR') === 0) echo "$msg\n";
				}
			}
			elseif (legiscan_option('ignore'))
			{
				$list = array();
				foreach (explode(',', legiscan_option('ignore')) as $bill_id)
				{
					$bill_id = (int) $bill_id;
					if ($bill_id)
					{
						$logic->ignore($bill_id);
						$list[] = $bill_id;
					}
				}
				if (legiscan_option('sync') && !empty($list))
				{
					$resp = $legiscan->setMonitor(array('list'=>$list,'action'=>'ignore'));
					foreach ($resp['return'] as $bill_id => $msg)
						if ($verbose || stripos($msg, 'ERROR') === 0) echo "$msg\n";
				}
			}
			elseif (legiscan_option('unmonitor'))
			{
				$list = array();
				foreach (explode(',', legiscan_option('unmonitor')) as $bill_id)
				{
					$bill_id = (int) $bill_id;
					if ($bill_id)
					{
						$logic->monitor($bill_id, false);
						$list[] = $bill_id;
					}
				}
				if (legiscan_option('sync') && !empty($list))
				{
					$resp = $legiscan->setMonitor(array('list'=>$list,'action'=>'remove'));
					foreach ($resp['return'] as $bill_id => $msg)
						if ($verbose || stripos($msg, 'ERROR') === 0) echo "$msg\n";
				}
			}
			elseif (legiscan_option('unignore'))
			{
				$list = array();
				foreach (explode(',', legiscan_option('unignore')) as $bill_id)
				{
					$bill_id = (int) $bill_id;
					if ($bill_id)
					{
						$logic->ignore($bill_id, false);
						$list[] = $bill_id;
					}
				}
				if (legiscan_option('sync') && !empty($list))
				{
					$resp = $legiscan->setMonitor(array('list'=>$list,'action'=>'remove'));
					foreach ($resp['return'] as $bill_id => $msg)
						if ($verbose || stripos($msg, 'ERROR') === 0) echo "$msg\n";
				}
			}
			elseif (legiscan_option('clean'))
			{
				if ($verbose) echo "Cleaning API cache\n";
				$cache = new LegiScan_Cache_File('api');
				$cache->clean($verbose);
			}
			elseif (legiscan_option('reset-db'))
			{
				$yes = legiscan_option('yes');
				if (!$yes) echo "WARNING: Continuing this action will reset the database to a clean install with only static lookups!\n\n";
				if ($yes || yes_no_prompt())
				{
					$table_list = array(
						'ls_bill',
						'ls_bill_amendment',
						'ls_bill_calendar',
						'ls_bill_history',
						'ls_bill_progress',
						'ls_bill_reason',
						'ls_bill_referral',
						'ls_bill_sast',
						'ls_bill_sponsor',
						'ls_bill_subject',
						'ls_bill_supplement',
						'ls_bill_text',
						'ls_bill_vote',
						'ls_bill_vote_detail',
						'ls_committee',
						'ls_ignore',
						'ls_monitor',
						'ls_people',
						'ls_session',
						'ls_signal',
						'ls_subject',
					);
					// Grab a direct handle to the database
					$db = $logic->getDB();
					foreach ($table_list as $table)
					{
						echo "Cleaning $table\n";
						$db->query("TRUNCATE TABLE $table");
					}
				}
				else
				{
					echo "Aborting.\n";
				}
			}

			if ($debug && $resp)
			{
				echo "API Response Object\n";
				print_r($resp);
			}
			
		} catch (APIException $e) {
			echo 'API Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			exit(1);

		} catch (APIAccessException $e) {
			echo 'API Access: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			exit(1);

		} catch (APIStatusException $e) {
			echo 'API Status: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			exit(1);

		} catch (PDOException $e) {
			echo 'Database Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			exit(1);

		} catch (Exception $e) {
			echo 'LegiScan Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			exit(1);
		}
	}
	// }}}
}
// }}}

// {{{ Sort out the command line options
$options = array(
	'monitorlist',
	'sessionlist:',
	'masterlist:',
	'bill:',
	'text:',
	'amendment:',
	'supplement:',
	'vote:',
	'people:',
	'search:',
	'state:',
	'score:',
	'import:',
	'sync',
	'monitor:',
	'unmonitor:',
	'ignore:',
	'unignore:',
	'stance:',
	'reset-db',
	'yes',
	'key:',
	'dry-run',
	'clean',
	'verbose',
	'debug',
);
$extra_help = '';
$extra_help .= "Command line interface to the LegiScan API that is capable of importing data, examples:\n";
$extra_help .= "  " . basename($argv[0]) . " --monitorlist --import changed\n";
$extra_help .= "  " . basename($argv[0]) . " --masterlist CA --import new\n";
$extra_help .= "  " . basename($argv[0]) . " --bill 823201\n";
$extra_help .= "  " . basename($argv[0]) . " --text 1565693\n";
$extra_help .= "  " . basename($argv[0]) . " --search \"citizens ADJ united\" --import new --score 75\n";
$extra_help .= "  " . basename($argv[0]) . " --monitor 852213 --stance support\n";
$extra_help .= "  " . basename($argv[0]) . " --clean --verbose\n";
legiscan_getopt($options, $extra_help);
// }}}

if (empty($legiscan_options))
	legiscan_help($options, "Missing OPTIONS\n" . $extra_help);

// Make sure the base options are there
if (!legiscan_option('key') && !preg_match('/^[0-9a-f]{32}$/i', LegiScan::getConfig('api_key')))
	legiscan_help($options, "Missing API Key (set in legiscan.conf or use --key)\n");

$cli = new LegiScan_CommandLine();

$cli->commands();
