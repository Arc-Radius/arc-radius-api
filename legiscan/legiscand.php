<?php
/**
 * A daemon process that will import and keep bills up to date
 *
 * @package LegiScan\Utility
 * @author LegiScan API Team <api@legiscan.com>
 * @license https://opensource.org/licenses/BSD-2-Clause
 * @copyright 2010-2020 LegiScan LLC
 * @link https://legiscan.com/datasets
 *
 */

// I say thee nay!
if (version_compare(PHP_VERSION, '5.4.0') < 0)
	die('PHP 5.4.0 or higher is required');

// Include the LegiScan API Client
require_once('LegiScan.php');

// {{{ LegiScan_Worker Class
/**
 * A daemon process that will import and keep bills up to date
 *
 * The `legiscand.php` script provides a daemon process that can use four
 * different methods, controlled by the `update_type` setting, to keep a
 * local database synchronized via LegiScan Pull API.
 *
 * ## Monitor
 * This mode will use the `ls_monitor` table to keep a specific `bill_id`
 * list updated. This list can be managed with `legiscan-cli.php`, though
 * determining the `bill_id` is an exercise for the reader and would
 * require additional scripting, though a likely source would be through
 * the search engine.
 *
 * <code>
 * php legiscan-cli.php --monitor 823882
 * php legiscan-cli.php --unmonitor 823882
 * </code>
 *
 * ## State
 * This mode will synchronize the entire master list from one or more
 * states. To set the state list edit `config.php` and add each state
 * to the `states[]` setting.
 *
 * For example to track all legislation in California and US Congress:
 * 
 * <code>
 * states[] = CA
 * states[] = US
 * </code>
 *
 * __NOTE__: It is *HIGHLY* recommended pre-loading the current state
 * [datasets](https://api.legiscan.com/dl) with `legiscan-bulk.php` prior
 * to the first run to minimize the on-boarding queries.
 *
 * ## Search (National)
 * This mode will synchronize the results of searches ran against the
 * national database. To specify the searches edit the `config.php`
 * and add each search to the `searches[]` setting.
 *
 * The searches will also be filtered by the global `relevance` cutoff
 * setting, which can be overridden on a per search basis by prepending
 * a different score and the pipe `|` character. In addition a `state`
 * abbreviations can also be prefixed to override either national or
 * state search. When used with a `relevance` override the `state`
 * should appear first separated by a comma `,`.
 *
 * Also notice that the entire search string should be quoted, and any
 * internal quotes should be escaped as `\"`.
 *
 * <code>
 * searches[] = "gender AND bathroom"
 * searches[] = "\"national popular vote\""
 * searches[] = "42|hemp OR cannabis OR marijuana"
 * searches[] = "NY|charter ADJ schools"
 * searches[] = "CA,60|vaccination AND status:passed"
 * </code>
 *
 * ## State Search
 * This mode combines both of the other methods such that the
 * `searches[]` are only ran against the `states[]` list,
 * unless a search specific `state` is used.
 *
 * @see LegiScan_Process
 * @see LegiScan_Pull
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_Worker
{
	/**
	 * Worker loop that generates a bill_id list then imports / updates
	 * via {@link LegiScan_Pull::importBillList}
	 *
	 * @param integer $daemon
	 *   If non-zero the worker will loop forever
	 *
	 */
	function worker($daemon)
	{
		try {
			// Create an instance to generate pull requests
			$legiscan = new LegiScan_Pull();

			// Create an instance to write to database
			$logic = new LegiScan_Process();

			// Grab a handle to the database
			$db = $logic->getDB();

			// {{{ Check and validate config
			$error_msg = '';

			$update_type = strtolower(LegiScan::getConfig('update_type'));
			$states = LegiScan::getConfig('states');
			$searches = LegiScan::getConfig('searches');
			$interval = LegiScan::getConfig('interval', 3600);
			$default_relevance = (int) LegiScan::getConfig('relevance', 50);
			$ignore_table = (bool) LegiScan::getConfig('use_ignore_table');

			$valid_types = array('monitored','state','search','state_search');
			if (!$update_type)
				$error_msg .= "Configuration value update_type is missing\n";
			elseif (!in_array($update_type, $valid_types))
				$error_msg .= "Invalid configuration value for update_type $update_type\n";

			if (!$default_relevance || !($default_relevance >= 0 && $default_relevance <= 100))
				$error_msg .= "Invalid configuration value for default_relevance $default_relevance\n";

			// At some point it would be better to run from cron...
			if ($interval < 3600)  $interval = 3600;
			if ($interval > 86400) $interval = 86400;

			if ($error_msg)
			{
				$msg = "Invalid Configuration\n\n$error_msg\nExiting\n";
				echo $msg;
				LegiScan::sendMail("LegiScan Daemon Error", $msg);
				exit(1);
			}
			// }}}

			do
			{
				// Reset the missing list and checked count each loop
				$logic->resetMissing();
				$checked = 0;

				// Build ignore list every run
				$ignore_list = array();
				if ($ignore_table)
				{
					$stmt = $db->prepare("SELECT bill_id FROM ls_ignore");
					$stmt->execute();
					while ($r = $stmt->fetch())
					{
						$ignore_list[] = $r['bill_id'];
					}
				}

				LegiScan::fileLog("LegiScanD starting $update_type update run");

				// {{{ Make a bill_id request list
				switch ($update_type)
				{
					// Specific bill_id list form ls_monitor table
					case 'monitored':
						$monitor_list = array();

						// Use state_abbr to tie getMasterListRaw to "current" session to
						// handle unmanaged ls_monitor entries for past sessions
						$sql = "SELECT m.bill_id, s.state_abbr
								FROM ls_monitor m
									INNER JOIN ls_bill b ON m.bill_id = b.bill_id
									INNER JOIN ls_state s ON b.state_id = s.state_id
								ORDER BY s.state_id, m.bill_id";
						$rs = $db->query($sql);
						while ($r = $rs->fetch())
						{
							$monitor_list[$r['state_abbr']][$r['bill_id']] = 1;
						}

						foreach (array_keys($monitor_list) as $state)
						{
							// Get current master list for $state
							$resp = $legiscan->getMasterListRaw($state);

							if ($resp['status'] == LegiScan::API_OK)
							{
								$session = array_shift($resp['masterlist']);

								foreach ($resp['masterlist'] as $bill)
								{
									// Compare master list to monitor list
									if (isset($monitor_list[$state][$bill['bill_id']]))
									{
										$checked++;
										$sql = "SELECT bill_id
												FROM ls_bill
												WHERE bill_id = :bill_id AND change_hash = :change_hash";
										$stmt = $db->prepare($sql);
										$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
										$stmt->bindValue(':change_hash', $bill['change_hash'], PDO::PARAM_STR);
										$stmt->execute();
										$exists = $stmt->fetchColumn();

										if (!$exists && !in_array($bill['bill_id'], $ignore_list))
										{
											$logic->request('bills', $bill['bill_id']);
										}
									}
								}
							}
						}

						break;

					// State full replication
					case 'state':
						if (in_array('all', $states))
						{
							$states = array();
							$state_list = $logic->getStateList();
							foreach ($state_list as $state)
								$states[] = $state['state_abbr'];	
						}

						foreach ($states as $state)
						{
							// Normally this would be a session_id, however this short cut
							// will mean the system always tracks "current" session

							$resp = $legiscan->getMasterListRaw($state);

							if ($resp['status'] == LegiScan::API_OK)
							{
								$session = array_shift($resp['masterlist']);

								foreach ($resp['masterlist'] as $bill)
								{
									$checked++;
									$sql = "SELECT bill_id
											FROM ls_bill
											WHERE bill_id = :bill_id AND change_hash = :change_hash";
									$stmt = $db->prepare($sql);
									$stmt->bindValue(':bill_id', $bill['bill_id'], PDO::PARAM_INT);
									$stmt->bindValue(':change_hash', $bill['change_hash'], PDO::PARAM_STR);
									$stmt->execute();
									$exists = $stmt->fetchColumn();

									if (!$exists && !in_array($bill['bill_id'], $ignore_list))
									{
										$logic->request('bills', $bill['bill_id']);
									}
								}
							}
						}
						break;

					// National searches
					case 'search':
						// To avoid largely duplicating code we reset the states
						// array to ALL for update_type=search and fall through
						// to the state_search code
						
						$states = array('ALL');

						// NOTE FALL THROUGH NO BREAK 

					// State searches
					case 'state_search':
						foreach ($states as $state)
						{
							foreach ($searches as $search)
							{
								$relevance = $default_relevance;
								$exhausted = false;
								$page = 1;

								// Never trust whitespace
								$search = trim($search);

								// Check for a state override and tidy up search string
								if (preg_match('#^([A-Z]{2})(\s*,\s*\d+)?\s*(\|.+)#i', $search, $m))
								{
									$state = strtoupper($m[1]);
									$search = ltrim(ltrim(str_replace(' ', '', $m[2]), ',') . $m[3], '|');
								}

								// Check for a relevance override and tidy up search string
								if (preg_match('#^(\d+)\s*\|\s*(.*)#', $search, $m))
								{
									$relevance = (int) $m[1];
									$search = $m[2];
								}

								// Drop any remaining whitespace
								$search = trim($search);

								do
								{
									// Use searchRaw to get 2000 results at a time since we only care
									// about relevance, bill_id and change_hash
									$params = array(
										'state' => $state,
										'query' => $search,
										'page' => $page,
									);

									$resp = $legiscan->getSearchRaw($params);

									if ($resp['status'] == LegiScan::API_OK && $resp['searchresult']['summary']['count'] > 0)
									{
										$summary = $resp['searchresult']['summary'];

										foreach ($resp['searchresult']['results'] as $result)
										{
											if ($result['relevance'] > $relevance)
											{
												$checked++;
												$sql = "SELECT bill_id
														FROM ls_bill
														WHERE bill_id = :bill_id AND change_hash = :change_hash";
												$stmt = $db->prepare($sql);
												$stmt->bindValue(':bill_id', $result['bill_id'], PDO::PARAM_INT);
												$stmt->bindValue(':change_hash', $result['change_hash'], PDO::PARAM_STR);
												$stmt->execute();
												$exists = $stmt->fetchColumn();

												if (!$exists && !in_array($result['bill_id'], $ignore_list))
												{
													$logic->request('bills', $result['bill_id']);
												}
											}
											else
											{
												$exhausted = true;
											}
										}

										// More pages or exhausted?
										if ($summary['page_total'] > $page)
											$page++;
										else
											$exhausted = true;
									}
									else
									{
										// Bad status or 0 count in results
										$exhausted = true;
									}
								} while (!$exhausted);
							}
						}
						break;
				}
				// }}}

				// Did we find any bills that were missing / changed
				$missing = $logic->getMissing();
				$cnt = isset($missing['bills']) ? count($missing['bills']) : 0;
				LegiScan::fileLog("LegiScanD found $cnt / $checked bills to process");

				// Do the thing!
				if (!empty($missing['bills']))
				{
					$legiscan->importBillList($missing['bills'], $logic);
					LegiScan::fileLog("LegiScanD processing complete");
				}

				// From the public pull perspective there are intrinsic cache delays
				// so we take a nice long nap until its time to make the donuts again
				if ($daemon)
					sleep($interval);

			} while ($daemon);

		} catch (APIException $e) {
			$msg = 'API Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			echo $msg;
			LegiScan::sendMail('LegiScan Daemon Error', $msg);
			exit(1);

		} catch (APIAccessException $e) {
			$msg =  'API Access: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			echo $msg;
			LegiScan::sendMail('LegiScan Daemon Error', $msg);
			exit(1);

		} catch (APIStatusException $e) {
			$msg =  'API Status: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			echo $msg;
			LegiScan::sendMail('LegiScan Daemon Error', $msg);
			exit(1);

		} catch (PDOException $e) {
			$msg = 'Database Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			echo $msg;
			LegiScan::sendMail('LegiScan Daemon Error', $msg);
			exit(1);

		} catch (Exception $e) {
			$msg = 'LegiScan Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			echo $msg;
			LegiScan::sendMail('LegiScan Daemon Error', $msg);
			exit(1);
		}
	}
}
// }}}


// Sort out the command line options
$options = array(
	'daemon',
);
legiscan_getopt($options);


$daemon = 0;
if (legiscan_option('daemon'))
	$daemon = 42; // just 'cause

$worker = new LegiScan_Worker();

$worker->worker($daemon);
