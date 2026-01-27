<?php
/**
 * A push endpoint server for the LegiScan API class
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

// Include the LegiScan API Client
require_once('LegiScan.php');

// {{{ LegiScan_Endpoint Class
// Wrapper class to facilitate code documenter
/**
 * A push endpoint server for LegiScan Push API subscriptions
 *
 * The `legiscan-push.php` script serves as the endpoint listener for LegiScan
 * Push API services. This will receive payloads from the LegiScan Push server
 * and process them into the provided database. The LegiScan API Client should
 * be installed in a location that is accessible by your web server and
 * externally by LegiScan servers.
 *
 * For additional authentication you may also set `api_auth_key` which can be
 * found / updated from the LegiScan API control panel at
 * [legiscan.com](https://legiscan.com/legiscan). This will validate against
 * the HTTP Authorization header sent with incoming payloads and reject those
 * with invalid tokens.
 *
 * If your API key is set to receive cooked application/x-www-form-urlencoded
 * payloads, be sure to change the `push_form_var` setting to the form
 * variable name specified in the API control panel.
 *
 * __NOTE__: To utilize this script you will need a Push API subscription. 
 *
 * @see LegiScan_Process
 * @see LegiScan_Push
 * @link https://api.legiscan.com/dl/
 *
 */
class LegiScan_Endpoint
{
	// {{{ process()
	/**
	 * Endpoint listener for LegiScan API Push subscriptions to process an
	 * incoming push payload to the database.
	 *
	 */
	function process()
	{
		// Process payload
		try {
			// Instance of class that will process an incoming push
			$handler = new LegiScan_Push();

			// Class to process payload into DB/storage and apply business rules
			$logic = new LegiScan_Process();

			// Holds any missing children (people, votes, texts, etc)
			$missing = array();

			// If incoming Authorization header is set validate it
			if (isset($_SERVER['HTTP_AUTHORIZATION']))
			{
				$auth_token = LegiScan::getConfig('api_auth_token');
				list($auth_type,$auth_value) = explode(' ', $_SERVER['HTTP_AUTHORIZATION']);
				if ($auth_token && $auth_type == 'Token' && (strtolower($auth_token) != strtolower($auth_value)))
				{
					throw new APIAuthTokenException('Authorization token does not match!');
				}
			}

			// Read the incoming data, decode it and set type
			$handler->processPush();

			// Get the actual payload data
			$payload = $handler->getPayload();

			// Process payload based on type
			switch ($handler->getPayloadType())
			{
				case 'bill':
					LegiScan::fileLog("LegiScan Push processing bill {$payload['bill']['bill_id']}");
					$missing = $logic->processBill($payload);
					foreach (array_keys($missing) as $key)
					{
						foreach ($missing[$key] as $id)
						{
							LegiScan::fileLog("LegiScan Push requesting $key $id");
						}
					}
					break;

				case 'roll_call':
					LegiScan::fileLog("LegiScan Push processing roll_call {$payload['roll_call']['roll_call_id']}");
					$logic->processRollCall($payload);
					$missing = $logic->getMissing();
					foreach (array_keys($missing) as $key)
					{
						foreach ($missing[$key] as $id)
						{
							LegiScan::fileLog("LegiScan Push requesting $key $id");
						}
					}
					break;

				case 'text':
					LegiScan::fileLog("LegiScan Push processing text {$payload['text']['doc_id']}");
					$logic->processBillText($payload);
					break;

				case 'amendment':
					LegiScan::fileLog("LegiScan Push processing amendment {$payload['amendment']['amendment_id']}");
					$logic->processAmendment($payload);
					break;

				case 'supplement':
					LegiScan::fileLog("LegiScan Push processing supplement {$payload['supplement']['supplement_id']}");
					$logic->processSupplement($payload);
					break;

				case 'person':
					LegiScan::fileLog("LegiScan Push processing person {$payload['person']['people_id']}");
					$logic->processPerson($payload);
					break;

				case 'session':
					LegiScan::fileLog("LegiScan Push processing session {$payload['session']['session_id']}");
					$logic->processSession($payload);
					break;

				default:
					throw new APIException('Unhandled payload type: ' . $handler->getPayloadType());
					break;
			}

			// Send a response
			$handler->respondOk($missing);
			
		} catch (APIAuthTokenException $e) {
			$error_msg = $e->getMessage();
			LegiScan::fileLog("LegiScan Push ERROR $error_msg");
			$handler->respondError($error_msg);

		} catch (APIAccessException $e) {
			$error_msg = $e->getMessage();
			LegiScan::fileLog("LegiScan Push ERROR $error_msg");
			$handler->respondError($error_msg);

		} catch (APIException $e) {
			$error_msg = "API Error: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog("LegiScan Push ERROR $error_msg");
			$handler->respondError($error_msg);

		} catch (PDOException $e) {
			$error_msg = "Database: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();;
			LegiScan::fileLog("LegiScan Push ERROR $error_msg");
			$handler->respondError($error_msg);

		} catch (Exception $e) {
			// Catch any other errors and push back an API error message
			$error_msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			LegiScan::fileLog("LegiScan Push ERROR $error_msg");
			$handler->respondError($error_msg);
		}
	}
	// }}}
}
// }}}

if (php_sapi_name() === 'cli')
	die("legiscan-push.php cannot be invoked from command line, it is a Push API subscription service endpoint.\n");

$endpoint = new LegiScan_Endpoint();

$endpoint->process();
