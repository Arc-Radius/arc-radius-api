<?php
/**
 * A simple web interface for testing the LegiScan API class
 *
 * @package LegiScan\Utility
 * @author LegiScan API Team <api@legiscan.com>
 * @license https://opensource.org/licenses/BSD-2-Clause
 * @copyright 2010-2020 LegiScan LLC
 *
 */

// No soup for you!
if (version_compare(PHP_VERSION, '5.4.0') < 0)
	die('PHP 5.4.0 or higher is required');

// Include the LegiScan API class
require_once('LegiScan.php');

// {{{ LegiScan_WebTest Class
// Wrapper class to facilitate code documenter
/**
 * A web interface for testing the LegiScan Pull API class
 *
 * The web interface is primarily for demonstration purposes to generate single
 * API requests to examine payload structure. The LegiScan API client and `legiscan-ui.php`
 * should be accessible by your web server. Then point your brower to the URL of
 * interface script
 *
 * <code>http://example.com/legiscan/legiscan-ui.php</code>
 *
 * Enter your API key in the space provided, select the type of payload request
 * you would like and enter the appropriate ID, then click `Run Test`.
 *
 * The system will then output the URL used to generate the request, the raw JSON
 * response and decoded payload data structure.
 *
 * @see LegiScan_Pull
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_WebTest
{
	// {{{ processPOST()
	/**
	 * Web based demo to provide a form to generate a Pull API call and dump the results
	 *
	 */
	function processPOST()
	{
		$keys = array('key', 'op', 'id');
		foreach ($keys as $k)
		{   
			$vals[$k] = null;
			if (isset($_POST[$k]))
				$$k = trim($_POST[$k]);
			else
				$$k = null;
		}

		// {{{ Draw the form
		echo "<html><head><title>LegiScan API Demo</title></head>\n";
		echo "<body style='font-family: sans-serif;'>\n";
		echo "<h2>LegiScan API Demo (Pull)</h2>\n";
		echo "<form method='post'>";
		echo "<input type='hidden' name='run' value='1'/>";
		echo "<table border='0'>";
		echo "<tr><td>API Key:</td><td><input type='text' size='34' maxlength='32' name='key' value='$key'></td></tr>\n";
		echo "<tr><td>&nbsp;</td></tr>\n";
		echo "<tr valign='top'>";
		echo "<td>Request Type:</td><td>";
		echo "<input type='radio' name='op' value='sessionlist' ".($op=='sessionlist'?'checked':'').">Session List (<i>state_abbr</i>)<br/>";
		echo "<input type='radio' name='op' value='master' ".($op=='master'?'checked':'').">Master List (<i>session_id</i>)<br/>";
		echo "<input type='radio' name='op' value='bill' ".($op=='bill'?'checked':'').">Bill (<i>bill_id</i>)<br/>";
		echo "<input type='radio' name='op' value='text' ".($op=='text'?'checked':'').">Text (<i>text_id</i>)<br/>";
		echo "<input type='radio' name='op' value='amendment' ".($op=='amendment'?'checked':'').">Amendment (<i>amendment_id</i>)<br/>";
		echo "<input type='radio' name='op' value='supplement' ".($op=='supplement'?'checked':'').">Supplement (<i>supplement_id</i>)<br/>";
		echo "<input type='radio' name='op' value='roll_call' ".($op=='roll_call'?'checked':'').">Vote (<i>roll_call_id</i>)<br/>";
		echo "<input type='radio' name='op' value='person' ".($op=='person'?'checked':'').">Person (<i>people_id</i>)<br/>";
		echo "</td>";
		echo "</tr>\n";
		echo "<tr><td>&nbsp;</td></tr>\n";
		echo "<tr><td>Request ID:</td><td ><input type='text' size='10' maxlength='7' name='id' value='$id'></td></tr>\n";
		echo "<tr valign='top'>";
		echo "</tr>\n";
		echo "</table>\n";
		echo "<br/>";
		echo "<input type='submit' value='Make API Request'>";
		echo "&nbsp;";
		echo "<input type='reset' value='Reset'><br/>";
		echo "</form>";
		echo "<pre>";
		// }}}

		// If running after a submit do something useful
		if (isset($_POST['run']))
		{
			// Make sure the required options are present
			if (!$key) die('<h2>LegiScan Error</h2>Missing API Key');
			if (!$op) die('<h2>LegiScan Error</h2>Missing Request Type');
			if (!$id) die('<h2>LegiScan Error</h2>Missing Request ID');

			try {
				// Create a new LegiScan instance with a key and encoding format, it should be noted
				// in real life the encoding paramater on API calls is optional and will default
				// to the preferred format associate with the API profile
				$legiscan = new LegiScan_Pull($key);

				// Determine which particular API call needs to happen and make it so
				switch ($op)
				{
					case 'sessionlist':
						$resp = $legiscan->getSessionList($id);
						break;
					case 'master':
						$resp = $legiscan->getMasterList($id);
						break;
					case 'bill':
						$resp = $legiscan->getBill($id);
						break;
					case 'text':
						$resp = $legiscan->getBillText($id);
						break;
					case 'amendment':
						$resp = $legiscan->getAmendment($id);
						break;
					case 'supplement':
						$resp = $legiscan->getSupplement($id);
						break;
					case 'roll_call':
						$resp = $legiscan->getRollCall($id);
						break;
					case 'person':
						$resp = $legiscan->getPerson($id);
						break;
				}

				// Print the resulting API object
				echo "<h2>API Reqeust URL</h2>";
				echo $legiscan->getURL() . "\n";
				echo "<h2>API JSON Response</h2>";
				echo $legiscan->getRawResponse() . "\n";
				echo "<h2>API Response Object</h2>";
				print_r($resp);

			} catch (APIException $e) {
				echo "<h2>LegiScan Error</h2>";
				echo 'API Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";

			} catch (APIAccessException $e) {
				echo "<h2>LegiScan Error</h2>";
				echo 'API Access: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";

			} catch (APIStatusException $e) {
				echo "<h2>LegiScan Error</h2>";
				echo 'API Status: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";

			} catch (PDOException $e) {
				echo "<h2>LegiScan Error</h2>";
				echo 'Database Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";

			} catch (Exception $e) {
				echo "<h2>LegiScan Error</h2>";
				echo 'Error: ' . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine() . "\n";
			}
		}
	}
	// }}}
}
// }}}

$webtest = new LegiScan_WebTest();

// Do the thing!
$webtest->processPOST();
