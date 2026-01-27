<?php
/**
 * A command line bulk loader and automatic synchronizer for JSON snapshots
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

// {{{ LegiScan_Import Class
// Wrapper class to facilitate code documenter
/**
 * A bulk loader for JSON datasets and snapshots
 *
 * Starting here is __STRONGLY__ encouraged as a few hundred files are the equivalent
 * of approximately __2 million__ individual API calls.
 *
 * Using the getDataset API hooks, a manually downloaded copy of a weekly
 * [Public Dataset](https://legiscan.com/datasets), or a custom subscriber onboarding
 * snapshot, this script will extract the contents of the archives and import/update
 * data as needed.
 *
 * When using getDataset, the script can operate in two modes, `--scan` and `--bulk`,
 * both of which import new and updated datasets.
 *
 * The `--scan` mode acts like a interactive search / browser with command line filters
 * to import eligible datasets. While in `--bulk` mode, the `states[]` and `years[]`
 * variables in `config.php` will be used to select which datasets will be
 * synchronized. The latter of which is most appropriate for scheduling for automating
 * updates.
 *
 * Show the command help
 *
 * <code>php legiscan-bulk.php --help</code>
 *
 * Use `config.php` settings to import datasets and answer yes to any prompts.
 *
 * <code>php legiscan-bulk.php --bulk --import --yes</code>
 *
 * Show a listing of all available datasets in California
 *
 * <code>php legiscan-bulk.php --scan --state CA</code>
 *
 * Import all new/changed datasets in 2020 with verbose output
 *
 * <code>php legiscan-bulk.php --scan --year 2020 --import --verbose</code>
 *
 * Import all new/changed 2016 special sessions in North Carolina
 *
 * <code>php legiscan-bulk.php --scan --state NC --special --year 2016 --import</code>
 *
 * Process `example.zip` but do not import, only show what would have been done
 *
 * <code>php legiscan-bulk.php --file example.zip --dry-run --debug</code>
 *
 * __NOTE__: This will not pull local copies of documents unless they are
 * included in the archive, though appropriate stub records will be created.
 *
 * @see LegiScan_Process
 * @see LegiScan_Bulk
 * @link https://api.legiscan.com/dl/
 * @link https://legiscan.com/datasets
 *
 */
class LegiScan_Import
{
	// {{{ Class Variables
	/**
	 * Instance of {@see LegiScan_Pull}
	 *
	 * @var LegiScan_Pull
	 */	
	protected $legiscan;
	/**
	 * Instance of {@see LegiScan_Process}
	 *
	 * @var LegiScan_Process
	 */	
	protected $logic;
	/**
	 * Instance of {@see LegiScan_Bulk}
	 *
	 * @var LegiScan_Bulk
	 */	
	protected $bulk;
	/**
	 * Database object {@see PDO}
	 *
	 * @var PDO
	 */	
	protected $db;
	// }}}

	// {{{ __construct()
	/** 
	 * Class constructor, initilizes necessary API instances
	 *
	 */
	function __construct()
	{
		$this->legiscan = new LegiScan_Pull();

		$this->logic = new LegiScan_Process();

		$this->db = $this->logic->getDB();

		$this->bulk = new LegiScan_Bulk();
	}
	// }}}

	// {{{ updateDatasets()
	/**
	 * Use the `states[]` and `years[]` settings in `config.php` to find eligible datasets
	 * from {@see LegiScan_Pull::getDatasetList} to pull and import to the local database
	 *
	 * @see LegiScan_Pull
	 *
	 */
	function updateDatasets()
	{
		$verbose = legiscan_option('verbose');
		$debug = legiscan_option('debug');
		$import = legiscan_option('import');
		$yes = legiscan_option('yes');

		try {
			// {{{ Variables & Config Check
			$checked = $skipped = $criteria = 0;
			$total_size = 0;
			$downloads = array();

			$error_msg = '';

			$states = LegiScan::getConfig('states');
			if (in_array('ALL', $states))
			{   
				$states = array();
				$state_list = $this->logic->getStateList();
				foreach ($state_list as $state)
					$states[] = $state['state_abbr'];
			}
			sort($states);
			if (empty($states))
				$error_msg .= "Empty states[] list\n";

			$years_all = false;
			$years = LegiScan::getConfig('years');
			if ($years)
			{
				if (in_array('ALL', $years))
					$years_all = true;
				if (in_array('CURRENT', $years))
				{
					$current_year = date('Y');
					if (!in_array($current_year, $years))
						$years[] = $current_year;
				}
			}
			else
			{
				$error_msg .= "Empty years[] list\n";
			}

			if ($error_msg)
			{
				$msg = "Invalid Configuration\n\n$error_msg\nExiting\n";
				echo $msg;
				exit(1);
			}
			// }}}

			// Go over each states[] from config.php
			foreach ($states as $state)
			{
				$filter = array('state'=>$state);
				$list = $this->legiscan->getDatasetList($filter);

				foreach ($list['datasetlist'] as $dataset)
				{
					$checked++;

					// Get the current import_hash to avoid duplicate downloads
					$sql = "SELECT import_hash
							FROM ls_session
							WHERE session_id = :session_id";
					$stmt = $this->db->prepare($sql);
					$stmt->bindValue(':session_id', $dataset['session_id'], PDO::PARAM_INT);
					$stmt->execute();
					$local_hash = $stmt->fetchColumn();

					$status = '----';

					if ($local_hash != $dataset['dataset_hash'])
					{
						if ($years_all || in_array($dataset['year_start'], $years) || in_array($dataset['year_end'], $years))
						{
							if ($local_hash)
								$status = 'Changed';
							else
								$status = 'New';
					
							$total_size += $dataset['dataset_size'];
							if ($debug)
								echo "Adding $state {$dataset['session_name']}\n";

							// Add state for convenience
							$dataset['state'] = $state;
							$downloads[] = $dataset;
						}
						else
						{
							$criteria++;
							if ($debug)
								echo "Ignoring $state {$dataset['session_name']} for year limits\n";
						}
					}
					else
					{
						$skipped++;
						$status = 'Same';
						if ($debug)
						{
							if ($years_all || in_array($dataset['year_start'], $years) || in_array($dataset['year_end'], $years))
								echo "Skipping $state {$dataset['session_name']} local copy up to date\n";
							else
								echo "Skipping $state {$dataset['session_name']} for year limits\n";
						}
					}

					if ($verbose)
					{
						$year = $dataset['year_start'];
						$year_end = $dataset['year_end'];
						if ($year != $year_end)
							$year .= '-' . $year_end;
						$year = '(' . $year . ')';

						echo sprintf("%10s\t%s\t%s\t%s\t%s\t%11s\t%-s\n",
							number_format($dataset['dataset_size']),
							$dataset['session_id'],
							$status,
							$dataset['dataset_date'],
							$state,
							$year,
							$dataset['session_title']
						);
					}
				}
			}

			if ($verbose)
				echo "Checked $checked archives: $skipped unchanged, $criteria ignored, " . count($downloads) . ' eligible (new ' . number_format($total_size) . " bytes)\n";

			if ($import && !empty($downloads))
			{
				if (!$yes && !yes_no_prompt())
				{
					echo "Ok, exiting, no changes were made\n";
					exit();
				}

				$this->processDownloads($downloads);
			}

			if ($verbose)
			{
				if ($import)
					echo "Update complete, " . count($downloads) . " archives updated\n";
				else
					echo "Update complete, " . count($downloads) . " archives eligible, no changes made (use --import)\n";
			}

		} catch (APIException $e) {
			$error_msg = "API Error: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";

		} catch (Exception $e) {
			// Catch any other errors and push back an API error message
			$error_msg = "LegiScan Import ERROR: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";
		}
	}
	// }}}

	// {{{ scanDatasets()
	/**
	 * Use {@see LegiScan_Pull::getDatasetList} to generate listings of available
	 * datasets with optional filtering and import new or updated archives
	 *
	 * @see LegiScan_Pull
	 *
	 */
	function scanDatasets()
	{
		$import = legiscan_option('import');
		$yes = legiscan_option('yes');

		$special = legiscan_option('special');
		$regular = legiscan_option('regular');

		$debug = legiscan_option('debug');
		$verbose = legiscan_option('verbose');

		try {
			$found = $checked = $criteria = $skipped = 0;
			$total_size = 0;
			$downloads = array();
			$session_list = $skip_list = array();
			$state_list = $this->logic->getStateList();

			$state = legiscan_option('state');
			$year = legiscan_option('year');
			$session = legiscan_option('session');
			if ($session)
			{
				if (stripos($session, ',') === false)
					$session_list[] = $session;
				else
					$session_list = explode(',', $session);
			}
			$skip = legiscan_option('skip');
			if ($skip)
			{
				if (stripos($skip, ',') === false)
					$skip_list[] = $skip;
				else
					$skip_list = explode(',', $skip);
			}

			$filter = array();
			if ($state)
				$filter['state'] = $state;
			if ($year)
				$filter['year'] = $year;

			$list = $this->legiscan->getDatasetList($filter);

			if (!empty($list['datasetlist']))
				echo sprintf("%2s\t%-9s\t%-26s\t%-4s\t%-7s\t%-6s\t%s\t%10s\n",
					'state',
					'year',
					'session',
					'sid',
					'flags',
					'status',
					'date',
					'size');

			foreach ($list['datasetlist'] as $dataset)
			{
				$found++;

				if (!empty($session_list) && !in_array($dataset['session_id'], $session_list))
					continue;
				if (!empty($skip_list) && in_array($dataset['session_id'], $skip_list))
					continue;
				if ($regular && $dataset['special'])
					continue;
				if ($special && !$dataset['special'])
					continue;

				$checked++;

				// Get the current import_hash to avoid duplicate downloads
				$sql = "SELECT import_hash
						FROM ls_session
						WHERE session_id = :session_id";
				$stmt = $this->db->prepare($sql);
				$stmt->bindValue(':session_id', $dataset['session_id'], PDO::PARAM_INT);
				$stmt->execute();
				$local_hash = $stmt->fetchColumn();

				$status = '----';

				if ($local_hash != $dataset['dataset_hash'])
				{
					if ($local_hash)
						$status = 'Changed';
					else
						$status = 'New';
					
					$total_size += $dataset['dataset_size'];
					if ($debug)
						echo "Adding $state {$dataset['session_name']}\n";

					$dataset['state'] = $state_list[$dataset['state_id']]['state_abbr'];
					$downloads[] = $dataset;
				}
				else
				{
					$skipped++;
					$status = 'Same';

					if ($debug)
						echo "Skipping $state {$dataset['session_name']} local copy up to date\n";
				}

				$flags = '(none)';
				if ($dataset['prefile'])
					$flags = 'Prefile';
				if ($dataset['sine_die'])
					$flags = 'SineDie';
				if ($dataset['prior'])
					$flags = 'Prior';
				$year = $dataset['year_start'];
				$year_end = $dataset['year_end'];
				if ($year != $year_end)
					$year .= '-' . $year_end;

				echo sprintf("%2s\t%-9s\t%-26s\t%4d\t%-7s\t%-6s\t%s\t%10s\n",
					$state_list[$dataset['state_id']]['state_abbr'],
					$year,
					$dataset['session_tag'],
					$dataset['session_id'],
					$flags,
					$status,
					$dataset['dataset_date'],
					number_format($dataset['dataset_size'])
				);
			}

			echo "\nFound $found archives: $checked checked, $skipped unchanged, " . count($downloads) . ' eligible (new ' . number_format($total_size) . " bytes)\n";

			if ($import && !empty($downloads))
			{
				if (!$yes && !yes_no_prompt())
				{
					echo "Ok, exiting, no changes were made\n";
					exit();
				}
				echo "\n";
				$this->processDownloads($downloads);
			}

		} catch (APIException $e) {
			$error_msg = "API Error: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";

		} catch (Exception $e) {
			// Catch any other errors and push back an API error message
			$error_msg = "LegiScan Import ERROR: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";
		}
	}
	// }}}

	// {{{ processFile()
	/**
	 * Process a single LegiScan session dataset or subscriber onboarding ZIP archive
	 * that was previously manually downloaded from the public or private site with
	 * {@see LegiScan_Bulk::importDataset}
	 *
	 * @link https://legiscan.com/datasets
	 * @see LegiScan_Bulk
	 */
	function processFile()
	{
		$verbose = legiscan_option('verbose');
		$debug = legiscan_option('debug');
		$dry_run = legiscan_option('dry-run');

		$zipfile = legiscan_option('file');

		try {
			if (!file_exists($zipfile))
				throw new Exception("File does not exist $zipfile");

			echo "Processing " . basename($zipfile) . " (" . number_format(filesize($zipfile)) . " bytes)\n";

			$params = array(
				'verbose' => $verbose,
				'debug' => $debug,
				'dry_run' => $dry_run,
				'expected_hash' => null
			);
			$hash = $this->bulk->importDataset($zipfile, $params);

		} catch (APIException $e) {
			$error_msg = "API Error: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";

		} catch (Exception $e) {
			// Catch any other errors and push back an API error message
			$error_msg = "LegiScan Import ERROR: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog($error_msg);
			echo "$error_msg\n";
		}
	}
	// }}}

	// {{{ processDownloads()
	/**
	 * Take array of eligible `dataset` objects from {@see scanDatasets} or {@see updateDatasets}
	 * and download the {@see getDataset} payloads, extract the ZIP file and import with
	 * {@see LegiScan_Bulk::importDataset}
	 *
	 * @param mixed[] $downloads
	 *   An array of `dataset` objects from getDatasetList to download and import
	 *
	 * @throws APIException
	 *
	 */
	private function processDownloads($downloads)
	{
		$verbose = legiscan_option('verbose');
		$debug = legiscan_option('debug');
		$dry_run = legiscan_option('dry-run');
		$scan = legiscan_option('scan');

		foreach ($downloads as $dl)
		{
			$payload = $this->legiscan->getDataset($dl['session_id'], $dl['access_key']);

			if (isset($payload['dataset']['zip']))
			{   
				$blob = base64_decode($payload['dataset']['zip']);
				unset($payload);
				if ($blob !== false)
				{
					$zip_file = tempnam(sys_get_temp_dir(), 'LegiScan-Zip');
					file_put_contents($zip_file, $blob);
					unset($blob);

					if ($verbose || $scan)
						echo "Processing {$dl['state']} {$dl['session_name']} (" . number_format($dl['dataset_size']) . " bytes) {$dl['dataset_hash']}\n";

					$params = array(
						'verbose' => $verbose,
						'debug' => $debug,
						'dry_run' => $dry_run,
						'expected_hash' => $dl['dataset_hash'],
					);
					$this->bulk->importDataset($zip_file, $params);

					if (!$debug)
						unlink($zip_file);
				}
				else
				{   
					throw new APIException("Could not decode dataset zip blob for ({$dl['session_id']}, {$dl['access_key']})");
				}
			}
			else
			{
				throw new APIException("Unrecognized session dataset for ({$dl['session_id']}, {$dl['access_key']})");
			}
		}

		return true;
	}
	// }}}
}
// }}}

// {{{ Sort out the command line options
$options = array(
	'bulk',
	'scan',
	'state:',
	'year:',
	'file:',
	'session:',
	'skip:',
	'special',
	'regular',
	'import',
	'yes',
	'verbose',
	'debug',
	'dry-run',
);
$extra_help = '';
$extra_help .= "Command line interface to the LegiScan API that is capable of importing data, examples:\n";
$extra_help .= "  " . basename($argv[0]) . " --bulk --import\n";
$extra_help .= "  " . basename($argv[0]) . " --scan\n";
$extra_help .= "  " . basename($argv[0]) . " --scan --state CA\n";
$extra_help .= "  " . basename($argv[0]) . " --scan --year 2020 --import\n";
$extra_help .= "  " . basename($argv[0]) . " --scan --session 1639\n";
legiscan_getopt($options, $extra_help);
// }}}

$import = new LegiScan_Import();

// Run based on config.php settings mode
if (legiscan_option('bulk'))
{
	$import->updateDatasets();
}
// Run based on command line filters
elseif (legiscan_option('scan'))
{
	$import->scanDatasets();
}
// Import a manually downloaded public dataset
elseif (legiscan_option('file'))
{
	$import->processFile();
}
else
{
	legiscan_help($options, $extra_help);
}
