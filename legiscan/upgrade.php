<?php

// Include the LegiScan API Client
require_once('LegiScan.php');

$release_version = LegiScan::VERSION;
$release_schema = LegiScan::SCHEMA_VERSION;

$logic = new LegiScan_Process();

global $db, $db_engine;

$db = $logic->getDB();

// {{{ Check database backend
$db_engine = '';
$dsn = LegiScan::getConfig('dsn');
if (stripos($dsn, 'mysql') === 0)
	$db_engine = 'mysql';
elseif (stripos($dsn, 'pgsql') === 0)
	$db_engine = 'pgsql';
elseif (stripos($dsn, 'sqlsrv') === 0)
	$db_engine = 'sqlsrv';
elseif (preg_match('#^([^:]+)#', $dsn, $m))
	die("Sorry, cannot automatically upgrade {$m[1]} backend\n");
// }}}

// {{{ check_missing_table()
function check_missing_table($table)
{
	global $db;
	$missing = false;
	try {
		$sql = "SELECT 1 FROM $table";
		$stmt = $db->prepare($sql);
		$stmt->execute();
	} catch (Exception $e) {
		$missing = true;
	}
	return $missing;
}
// }}}
// {{{ check_missing_column()
function check_missing_column($table, $col)
{
	global $db;
	$missing = false;
	try {
		$sql = "SELECT $col FROM $table";
		$stmt = $db->prepare($sql);
		$stmt->execute();
	} catch (Exception $e) {
		$missing = true;
	}
	return $missing;
}
// }}}
// {{{ check_missing_value()
function check_missing_value($table, $col, $value)
{
	global $db;
	$missing = true;
	try {
		$sql = "SELECT 1 FROM $table WHERE $col = :value";
		$stmt = $db->prepare($sql);
		if (is_numeric($value))
			$stmt->bindValue(':value', $value, PDO::PARAM_INT);
		else
			$stmt->bindValue(':value', $value, PDO::PARAM_STR);
		$stmt->execute();
		if ($stmt->fetchColumn())
			$missing = false;
	} catch (Exception $e) {
		$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
		throw new Exception($msg);
	}
	return $missing;
}
// }}}
// {{{ check_rows_exist()
function check_rows_exist($table, $where)
{
	global $db;

	$exists = false;
	try {
		$sql = "SELECT 1 FROM $table WHERE $where";
		$stmt = $db->prepare($sql);
		$stmt->execute();
		if ($stmt->fetchColumn())
			$exists = true;
	} catch (Exception $e) {
		$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
		throw new Exception($msg);
	}
	return $exists;
}
// }}}
// {{{ get_var()
function get_var($name)
{
	global $db;
	$sql = "SELECT value FROM ls_variable WHERE name = :name";
	$stmt = $db->prepare($sql);
	$stmt->bindValue(':name', $name, PDO::PARAM_STR);
	$stmt->execute();
	return json_decode($stmt->fetchColumn());
}
// }}}
// {{{ set_var()
function set_var($name, $value)
{
	global $db;
	$sql = "UPDATE ls_variable SET value = :value WHERE name = :name";
	$stmt = $db->prepare($sql);
	$stmt->bindValue(':name', $name, PDO::PARAM_STR);
	$stmt->bindValue(':value', json_encode($value), PDO::PARAM_STR);
	$stmt->execute();
	return true;
}
// }}}

// {{{ Check ls_variable table
if (check_missing_table('ls_variable'))
{
	try {
		echo "Creating ls_variable\n";
		$sql = "CREATE TABLE ls_variable (name VARCHAR(64) NOT NULL, value TEXT NOT NULL)";
		$db->prepare($sql)->execute();

		$sql = "INSERT INTO ls_variable (name, value) VALUES (:name, :value)";
		$stmt = $db->prepare($sql);
		$stmt->bindValue(':name', 'version', PDO::PARAM_STR);
		$stmt->bindValue(':value', '1', PDO::PARAM_STR);
		$stmt->execute();
		$stmt->bindValue(':name', 'schema', PDO::PARAM_STR);
		$stmt->bindValue(':value', '1', PDO::PARAM_STR);
		$stmt->execute();

		$sql = "ALTER TABLE ls_variable ADD PRIMARY KEY (name)";
		$db->prepare($sql)->execute();
	} catch (Exception $e) {
		print_r($e);
		$error_msg = "LegiScan Import ERROR: " . $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
		echo "$error_msg\n";
	}
}
// }}}

$schema = get_var('schema');
$version = get_var('version');

// {{{ Version 1.4.0
if (version_compare($version, '1.4.0') < 0)
{
	try {
		// Postgres does not like the check_ functions...
		if ($db_engine == 'mysql')
			$db->beginTransaction();

		echo "Upgrading $version -> 1.4.0\n";

		if (check_missing_table('ls_bill_referral'))
		{
			echo "Adding table ls_bill_referral\n";
			if ($db_engine == 'mysql')
			{
				$sql = "CREATE TABLE ls_bill_referral (bill_id mediumint(8) UNSIGNED NOT NULL, referral_step tinyint(3) UNSIGNED NOT NULL, referral_date date DEFAULT NULL, committee_id smallint(5) UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8";
			}
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
			{
				$sql = "CREATE TABLE ls_bill_referral (bill_id integer NOT NULL, referral_step smallint NOT NULL, referral_date date DEFAULT NULL, committee_id smallint NOT NULL)";
			}
			$db->prepare($sql)->execute();
			$sql = "ALTER TABLE ls_bill_referral ADD PRIMARY KEY (bill_id,referral_step)";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('ls_stance'))
		{
			echo "Adding table ls_stance\n";
			if ($db_engine == 'mysql')
			{
				$sql = "CREATE TABLE ls_stance (stance tinyint(3) UNSIGNED NOT NULL, stance_desc varchar(24) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8";
			}
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
			{
				$sql = "CREATE TABLE ls_stance (stance smallint NOT NULL, stance_desc varchar(24) NOT NULL)";
			}
			$db->prepare($sql)->execute();
			$sql = "ALTER TABLE ls_stance ADD PRIMARY KEY (stance)";
			$db->prepare($sql)->execute();
			$sql = "INSERT INTO ls_stance (stance, stance_desc) VALUES (0, 'Watch'), (1, 'Support'), (2, 'Oppose')";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_amendment', 'local_fragment'))
		{
			echo "Adding local_fragment column to ls_bill_amendment\n";
			$sql = "ALTER TABLE ls_bill_amendment ADD COLUMN local_fragment VARCHAR(255) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_amendment', 'amendment_size'))
		{
			echo "Adding amendment_size column to ls_bill_amendment\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_amendment ADD COLUMN amendment_size mediumint(8) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_bill_amendment ADD COLUMN amendment_size integer NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
			echo "Adding amendment_hash column to ls_bill_amendment\n";
			$sql = "ALTER TABLE ls_bill_amendment ADD COLUMN amendment_hash char(32) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_sast', 'sast_bill_number'))
		{
			echo "Adding sast_bill_number column to ls_bill_sast\n";
			$sql = "ALTER TABLE ls_bill_sast ADD COLUMN sast_bill_number VARCHAR(10) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_supplement', 'local_fragment'))
		{
			echo "Adding local_fragment column to ls_bill_supplement\n";
			$sql = "ALTER TABLE ls_bill_supplement ADD COLUMN local_fragment VARCHAR(255) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_supplement', 'supplement_size'))
		{
			echo "Adding supplement_size column to ls_bill_supplement\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_supplement ADD COLUMN supplement_size mediumint(8) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_bill_supplement ADD COLUMN supplement_size integer NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
			echo "Adding supplement_hash column to ls_bill_supplement\n";
			$sql = "ALTER TABLE ls_bill_supplement ADD COLUMN supplement_hash char(32) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_text', 'local_fragment'))
		{
			echo "Adding local_fragment column to ls_bill_text\n";
			$sql = "ALTER TABLE ls_bill_text ADD COLUMN local_fragment VARCHAR(255) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_bill_text', 'bill_text_size'))
		{
			echo "Adding bill_text_size column to ls_bill_text\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_text ADD COLUMN bill_text_size mediumint(8) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_bill_text ADD COLUMN bill_text_size integer NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
			echo "Adding bill_text_hash column to ls_bill_text\n";
			$sql = "ALTER TABLE ls_bill_text ADD COLUMN bill_text_hash char(32) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_mime_type', 'mime_ext'))
		{
			echo "Adding mime_ext column to ls_mime_type\n";
			$sql = "ALTER TABLE ls_mime_type ADD COLUMN mime_ext VARCHAR(4) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_monitor', 'stance'))
		{
			echo "Adding stance column to ls_monitor\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_monitor ADD COLUMN stance tinyint(3) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_monitor ADD COLUMN stance smallint NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_people', 'knowwho_pid'))
		{
			echo "Adding knowwho_pid column to ls_people\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_people ADD COLUMN knowwho_pid mediumint(8) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_people ADD COLUMN knowwho_pid integer NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_session', 'import_hash'))
		{
			echo "Adding import_hash column to ls_session\n";
			$sql = "ALTER TABLE ls_session ADD COLUMN import_hash CHAR(32) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_session', 'session_tag'))
		{
			echo "Adding session_tag column to ls_session\n";
			$sql = "ALTER TABLE ls_session ADD COLUMN session_tag VARCHAR(32) DEFAULT NULL";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_session', 'prefile'))
		{
			echo "Adding prefile column to ls_session\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_session ADD COLUMN prefile tinyint(3) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_session ADD COLUMN prefile smallint NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_session', 'sine_die'))
		{
			echo "Adding sine_die column to ls_session\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_session ADD COLUMN sine_die tinyint(3) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_session ADD COLUMN sine_die smallint NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
		}

		if (check_missing_column('ls_session', 'prior'))
		{
			echo "Adding prior column to ls_session\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_session ADD COLUMN prior tinyint(3) UNSIGNED NOT NULL DEFAULT 0";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "ALTER TABLE ls_session ADD COLUMN prior smallint NOT NULL DEFAULT 0";
			$db->prepare($sql)->execute();
		}

		if (check_rows_exist('ls_bill_amendment', 'local_copy = 1 AND local_fragment IS NULL'))
		{
			echo "Updating local_fragment values in ls_bill_amendment\n";
			$sql = "SELECT amendment_id FROM ls_bill_amendment WHERE local_copy = 1 AND local_fragment IS NULL";
			$rs = $db->prepare($sql);
			$rs->execute();
			
			$sql = "UPDATE ls_bill_amendment SET local_fragment = :local_fragment WHERE amendment_id = :amendment_id";
			$stmt = $db->prepare($sql);

			while ($r = $rs->fetch())
			{
				$local_fragment = $logic->getCacheFilename('amendment', $r['amendment_id']);
				$stmt->bindValue(':local_fragment', $local_fragment, PDO::PARAM_STR);
				$stmt->bindValue(':amendment_id', $r['amendment_id'], PDO::PARAM_INT);
				$stmt->execute();
			}
		}

		if (check_rows_exist('ls_bill_supplement', 'local_copy = 1 AND local_fragment IS NULL'))
		{
			echo "Updating local_fragment values in ls_bill_supplement\n";
			$sql = "SELECT supplement_id FROM ls_bill_supplement WHERE local_copy = 1 AND local_fragment IS NULL";
			$rs = $db->prepare($sql);
			$rs->execute();
			
			$sql = "UPDATE ls_bill_supplement SET local_fragment = :local_fragment WHERE supplement_id = :supplement_id";
			$stmt = $db->prepare($sql);

			while ($r = $rs->fetch())
			{
				$local_fragment = $logic->getCacheFilename('supplement', $r['supplement_id']);
				$stmt->bindValue(':local_fragment', $local_fragment, PDO::PARAM_STR);
				$stmt->bindValue(':supplement_id', $r['supplement_id'], PDO::PARAM_INT);
				$stmt->execute();
			}
		}

		if (check_rows_exist('ls_bill_text', 'local_copy = 1 AND local_fragment IS NULL'))
		{
			echo "Updating local_fragment values in ls_bill_text\n";
			$sql = "SELECT text_id FROM ls_bill_text WHERE local_copy = 1 AND local_fragment IS NULL";
			$rs = $db->prepare($sql);
			$rs->execute();
			
			$sql = "UPDATE ls_bill_text SET local_fragment = :local_fragment WHERE text_id = :text_id";
			$stmt = $db->prepare($sql);

			while ($r = $rs->fetch())
			{
				$local_fragment = $logic->getCacheFilename('text', $r['text_id']);
				$stmt->bindValue(':local_fragment', $local_fragment, PDO::PARAM_STR);
				$stmt->bindValue(':text_id', $r['text_id'], PDO::PARAM_INT);
				$stmt->execute();
			}
		}

		if (check_missing_value('ls_mime_type', 'mime_id', 11))
		{
			echo "Rebuilding ls_mime_type values\n";
			$sql = "TRUNCATE ls_mime_type";
			$db->prepare($sql)->execute();
			$sql = "INSERT INTO ls_mime_type (mime_id, mime_type, mime_ext, is_binary) VALUES
				(1, 'text/html', 'html', 0),
				(2, 'application/pdf', 'pdf', 1),
				(3, 'application/wordperfect', 'wpd', 1),
				(4, 'application/msword', 'doc', 1),
				(5, 'application/rtf', 'rtf', 1),
				(6, 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'docx', 1),
				(7, 'application/vnd.ms-excel', 'xls', 1),
				(8, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'xlsx', 1),
				(9, 'text/csv', 'csv', 0),
				(10, 'application/json', 'json', 0),
				(11, 'application/zip', 'zip', 1)";
			$db->prepare($sql)->execute();
		}

		if (check_missing_value('ls_progress', 'progress_event_id', 0))
		{
			echo "Updating ls_progress for Prefile\n";
			$sql = "INSERT INTO ls_progress (progress_event_id, progress_desc) VALUES (0, 'Prefile')";
			$db->prepare($sql)->execute();
		}

		if (!check_missing_value('ls_type', 'bill_type_id', 0))
		{
			echo "Cleaning ls_type\n";
			$sql = "DELETE FROM ls_type WHERE bill_type_id = 0";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill'))
		{
			echo "Creating view lsv_bill\n";
			$sql = "CREATE VIEW lsv_bill AS SELECT b.bill_id, st.state_abbr, b.bill_number, b.status_id, p.progress_desc AS status_desc, b.status_date, b.title, b.description, b.bill_type_id, t.bill_type_name, t.bill_type_abbr, b.body_id, bo1.body_abbr, bo1.body_short, bo1.body_name, b.current_body_id, bo2.body_abbr AS current_body_abbr, bo2.body_short AS current_body_short, bo2.body_name AS current_body_name, b.pending_committee_id, c.committee_body_id AS pending_committee_body_id, bo3.body_abbr AS pending_committee_body_abbr, bo3.body_short AS pending_committee_body_short, bo3.body_name AS pending_committee_body_name, c.committee_name AS pending_committee_name, b.legiscan_url, b.state_url, b.change_hash, b.created, b.updated, b.state_id, st.state_name, b.session_id, s.year_start AS session_year_start, s.year_end AS session_year_end, s.prefile AS session_prefile, s.sine_die AS session_sine_die, s.prior AS session_prior, s.special AS session_special, s.session_tag, s.session_title, s.session_name FROM ls_bill b INNER JOIN ls_type t ON b.bill_type_id = t.bill_type_id INNER JOIN ls_session s ON b.session_id = s.session_id INNER JOIN ls_body bo1 ON b.body_id = bo1.body_id INNER JOIN ls_body bo2 ON b.current_body_id = bo2.body_id LEFT JOIN ls_committee c ON b.pending_committee_id = c.committee_id LEFT JOIN ls_body bo3 ON c.committee_body_id = bo3.body_id LEFT JOIN ls_progress p ON b.status_id = p.progress_event_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_amendment'))
		{
			echo "Creating view lsv_bill_amendment\n";
			$sql = "CREATE VIEW lsv_bill_amendment AS SELECT b.bill_id, st.state_abbr, b.bill_number, ba.amendment_id, ba.amendment_date, ba.amendment_body_id, ba.amendment_title, ba.amendment_desc, ba.amendment_size, ba.adopted, ba.amendment_mime_id, mt.mime_type, mt.mime_ext, ba.amendment_hash, ba.legiscan_url, ba.state_url, ba.local_copy, ba.local_fragment, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id, ba.created, ba.updated FROM ls_bill b INNER JOIN ls_bill_amendment ba ON b.bill_id = ba.bill_id INNER JOIN ls_mime_type mt ON ba.amendment_mime_id = mt.mime_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_calendar'))
		{
			echo "Creating view lsv_bill_calendar\n";
			$sql = "CREATE VIEW lsv_bill_calendar AS SELECT b.bill_id, st.state_abbr, b.bill_number, bc.event_date, bc.event_time, bc.event_location, bc.event_desc, bc.event_type_id, et.event_type_desc, bc.event_hash, b.pending_committee_id, c.committee_body_id AS pending_committee_body_id, bo.body_abbr AS pending_committee_body_abbr, bo.body_short AS pending_committee_body_short, c.committee_name AS pending_committee_name, bc.created, bc.updated, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id FROM ls_bill b INNER JOIN ls_bill_calendar bc ON b.bill_id = bc.bill_id INNER JOIN ls_event_type et ON bc.event_type_id = et.event_type_id LEFT JOIN ls_committee c ON b.pending_committee_id = c.committee_id LEFT JOIN ls_body bo ON c.committee_body_id = bo.body_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_history'))
		{
			echo "Creating view lsv_bill_history\n";
			$sql = "CREATE VIEW lsv_bill_history AS SELECT b.bill_id, st.state_abbr, b.bill_number, bh.history_step, bh.history_date, bo.body_short AS history_body_short, bh.history_action, bh.history_body_id, bo.body_abbr AS history_body_abbr, bo.body_name AS history_body_name, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id FROM ls_bill b INNER JOIN ls_bill_history bh ON b.bill_id = bh.bill_id LEFT JOIN ls_body bo ON bh.history_body_id = bo.body_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_reason'))
		{
			echo "Creating view lsv_bill_reason\n";
			$sql = "CREATE VIEW lsv_bill_reason AS SELECT b.bill_id, st.state_abbr, b.bill_number, br.reason_id, r.reason_desc, br.created as change_time, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id FROM ls_bill b INNER JOIN ls_bill_reason br ON b.bill_id = br.bill_id INNER JOIN ls_reason r ON br.reason_id = r.reason_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_referral'))
		{
			echo "Creating view lsv_bill_referral\n";
			$sql = "CREATE VIEW lsv_bill_referral AS SELECT b.bill_id, st.state_abbr, b.bill_number, br.referral_step, br.referral_date, br.committee_id AS referral_committee_id, bo1.body_abbr AS referral_committee_body_abbr, bo1.body_short AS referral_committee_body_short, bo1.body_name AS referral_committee_body_name, c1.committee_name AS referral_committee_name, b.pending_committee_id, bo2.body_abbr AS pending_committee_body_abbr, bo2.body_short AS pending_committee_body_short, bo2.body_name AS pending_committee_body_name, c2.committee_name AS pending_committee_name, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id FROM ls_bill b INNER JOIN ls_bill_referral br ON b.bill_id = br.bill_id LEFT JOIN ls_committee c1 ON br.committee_id = c1.committee_id INNER JOIN ls_body bo1 ON c1.committee_body_id = bo1.body_id LEFT JOIN ls_committee c2 ON b.pending_committee_id = c2.committee_id LEFT JOIN ls_body bo2 ON c2.committee_body_id = bo2.body_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_sast'))
		{
			echo "Creating view lsv_bill_sast\n";
			$sql = "CREATE VIEW lsv_bill_sast AS SELECT b1.bill_id, st1.state_abbr, b1.bill_number, bs.sast_type_id, sty.sast_description, b1.state_id, b1.session_id, b1.body_id, b1.current_body_id, b1.bill_type_id, b1.status_id, b1.pending_committee_id, bs.sast_bill_id, st2.state_abbr AS sast_state_abbr, bs.sast_bill_number, b2.state_id AS sast_state_id, b2.session_id AS sast_session_id, b2.body_id AS sast_body_id, b2.current_body_id AS sast_current_body_id, b2.bill_type_id AS sast_bill_type_id, b2.status_id AS sast_status_id, b2.pending_committee_id AS sast_pending_committee_id FROM ls_bill b1 INNER JOIN ls_bill_sast bs ON b1.bill_id = bs.bill_id INNER JOIN ls_sast_type sty ON bs.sast_type_id = sty.sast_id INNER JOIN ls_state st1 ON b1.state_id = st1.state_id LEFT JOIN ls_bill b2 ON bs.sast_bill_id = b2.bill_id LEFT JOIN ls_state st2 ON b2.state_id = st2.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_sponsor'))
		{
			echo "Creating view lsv_bill_sponsor\n";
			$sql = "CREATE VIEW lsv_bill_sponsor AS SELECT b.bill_id, st.state_abbr, b.bill_number, bs.people_id, bs.sponsor_order, bs.sponsor_type_id, spt.sponsor_type_desc, p.party_id, pa.party_abbr, pa.party_name, p.role_id, r.role_abbr, r.role_name, p.name, p.first_name, p.middle_name, p.last_name, p.suffix, p.nickname, p.ballotpedia, p.followthemoney_eid, p.votesmart_id, p.opensecrets_id, p.knowwho_pid, p.committee_sponsor_id, c.committee_body_id AS committee_sponsor_body_id, c.committee_name AS committee_sponsor_name, p.person_hash, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id FROM ls_bill b INNER JOIN ls_bill_sponsor bs ON b.bill_id = bs.bill_id INNER JOIN ls_sponsor_type spt ON bs.sponsor_type_id = spt.sponsor_type_id INNER JOIN ls_people p ON bs.people_id = p.people_id LEFT JOIN ls_committee c ON p.committee_sponsor_id = c.committee_id INNER JOIN ls_party pa ON p.party_id = pa.party_id INNER JOIN ls_role r ON p.role_id = r.role_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_subject'))
		{
			echo "Creating view lsv_bill_subject\n";
			$sql = "CREATE VIEW lsv_bill_subject AS SELECT b.bill_id, st.state_abbr, b.bill_number, bs.subject_id, s.subject_name, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id FROM ls_bill b INNER JOIN ls_bill_subject bs ON b.bill_id = bs.bill_id INNER JOIN ls_subject s ON bs.subject_id = s.subject_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_supplement'))
		{
			echo "Creating view lsv_bill_supplement\n";
			$sql = "CREATE VIEW lsv_bill_supplement AS SELECT b.bill_id, st.state_abbr, b.bill_number, bs.supplement_id, bs.supplement_date, bs.supplement_type_id, sut.supplement_type_desc, bs.supplement_size, bs.supplement_mime_id, mt.mime_type, mt.mime_ext, bs.supplement_hash, bs.legiscan_url, bs.state_url, bs.local_copy, bs.local_fragment, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id, bs.created, bs.updated FROM ls_bill b INNER JOIN ls_bill_supplement bs ON b.bill_id = bs.bill_id INNER JOIN ls_supplement_type sut ON bs.supplement_type_id = sut.supplement_type_id INNER JOIN ls_mime_type mt ON bs.supplement_mime_id = mt.mime_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_text'))
		{
			echo "Creating view lsv_bill_text\n";
			$sql = "CREATE VIEW lsv_bill_text AS SELECT b.bill_id, st.state_abbr, b.bill_number, bt.text_id, bt.bill_text_size, bt.bill_text_date, bt.bill_text_type_id, tt.bill_text_name, tt.bill_text_sort, bt.bill_text_mime_id, mt.mime_type, mt.mime_ext, bt.bill_text_hash, bt.legiscan_url, bt.state_url, bt.local_copy, bt.local_fragment, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id, bt.created, bt.updated FROM ls_bill b INNER JOIN ls_bill_text bt ON b.bill_id = bt.bill_id INNER JOIN ls_text_type tt ON bt.bill_text_type_id = tt.bill_text_type_id INNER JOIN ls_mime_type mt ON bt.bill_text_mime_id = mt.mime_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_vote'))
		{
			echo "Creating view lsv_bill_vote\n";
			$sql = "SELECT b.bill_id, st.state_abbr, b.bill_number, bv.roll_call_id, bv.roll_call_date, bv.roll_call_desc, bv.roll_call_body_id, bo.body_abbr AS roll_call_body_abbr, bo.body_short AS roll_call_body_short, bo.body_name AS roll_call_body_name, bv.yea, bv.nay, bv.nv, bv.absent, bv.total, bv.passed, bv.legiscan_url, bv.state_url, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id, bv.created, bv.updated FROM ls_bill b INNER JOIN ls_bill_vote bv ON b.bill_id = bv.bill_id INNER JOIN ls_body bo ON bv.roll_call_body_id = bo.body_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if (check_missing_table('lsv_bill_vote_detail'))
		{
			echo "Creating view lsv_bill_vote_detail\n";
			$sql = "CREATE VIEW lsv_bill_vote_detail AS SELECT b.bill_id, st.state_abbr, b.bill_number, bv.roll_call_id, bvd.people_id, bvd.vote_id, v.vote_desc, p.party_id, pa.party_abbr, p.role_id, r.role_abbr, r.role_name, p.name, p.first_name, p.middle_name, p.last_name, p.suffix, p.nickname, p.ballotpedia, p.followthemoney_eid, p.votesmart_id, p.opensecrets_id, p.knowwho_pid, p.person_hash, b.state_id, st.state_name, b.session_id, b.body_id, b.current_body_id, b.bill_type_id, b.status_id, b.pending_committee_id FROM ls_bill b INNER JOIN ls_bill_vote bv ON b.bill_id = bv.bill_id INNER JOIN ls_bill_vote_detail bvd ON bv.roll_call_id = bvd.roll_call_id INNER JOIN ls_vote v ON bvd.vote_id = v.vote_id INNER JOIN ls_people p ON bvd.people_id = p.people_id INNER JOIN ls_party pa ON p.party_id = pa.party_id INNER JOIN ls_role r ON p.role_id = r.role_id INNER JOIN ls_state st ON b.state_id = st.state_id";
			$db->prepare($sql)->execute();
		}

		if ($db_engine == 'mysql')
		{
			echo "Renaming MySQL indexes...\n";
			$rename = array(
				array('ls_bill', 'bill_number'),
				array('ls_bill', 'session_id'),
				array('ls_bill', 'state_id'),
				array('ls_bill_amendment', 'bill_id'),
				array('ls_bill_reason', 'bill_id'),
				array('ls_bill_supplement', 'bill_id'),
				array('ls_bill_text', 'bill_id'),
				array('ls_bill_vote', 'bill_id'),
				array('ls_body', 'body_abbr'),
				array('ls_body', 'role_id'),
				array('ls_body', 'state_id'),
				array('ls_committee', 'body_id', 'committee_body_id'),
				array('ls_people', 'party_id'),
				array('ls_people', 'role_id'),
				array('ls_people', 'state_id'),
				array('ls_state', 'state_abbr'),
				array('ls_subject', 'state_id'),
			);
			foreach ($rename as $k)
			{
				$table = $k[0];
				$fld = $k[1];
				$on_fld = $fld;
				$idx_name = $table . '_' . $fld . '_idx';
				if (isset($k[2]))
				{
					$on_fld = $k[2];
					$idx_name = $table . '_' . $on_fld . '_idx';
				}
				echo "$idx_name\n";
				$sql = "ALTER TABLE {$table} DROP INDEX {$fld}";
				$db->prepare($sql)->execute();
				$sql = "CREATE INDEX {$idx_name} ON {$table} ({$on_fld})";
				$db->prepare($sql)->execute();
			}
		}

		try {
			echo "Re-indexing ls_bill_sast\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_sast DROP INDEX bill_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_sast_bill_id_idx";
			$db->prepare($sql)->execute();
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_sast DROP INDEX sast_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_sast_sast_bill_id_idx";
			$db->prepare($sql)->execute();
			$sql = "ALTER TABLE ls_bill_sast ADD PRIMARY KEY (bill_id,sast_type_id,sast_bill_id)";
			$db->prepare($sql)->execute();
		} catch (Exception $e) {
			$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			echo "Error indexing ls_bill_sast: $msg\n";
		}

		try {
			echo "Re-indexing ls_bill_subject\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_subject DROP INDEX bill_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_subject_bill_id_idx";
			$db->prepare($sql)->execute();
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_subject DROP INDEX subject_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_subject_subject_id_idx";
			$db->prepare($sql)->execute();
			$sql = "ALTER TABLE ls_bill_subject ADD PRIMARY KEY (bill_id,subject_id)";
			$db->prepare($sql)->execute();
		} catch (Exception $e) {
			$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			echo "Error indexing ls_bill_subject: $msg\n";
		}

		try {
			echo "Re-indexing ls_bill_vote_detail\n";
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_vote_detail DROP INDEX people_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_vote_detail_people_id_idx";
			$db->prepare($sql)->execute();
			if ($db_engine == 'mysql')
				$sql = "ALTER TABLE ls_bill_vote_detail DROP INDEX roll_call_id";
			elseif ($db_engine == 'pgsql' || $db_engine == 'sqlsrv')
				$sql = "DROP INDEX ls_bill_vote_detail_roll_call_id_idx";
			$db->prepare($sql)->execute();
			$sql = "ALTER TABLE ls_bill_vote_detail ADD PRIMARY KEY (roll_call_id,people_id)";
			$db->prepare($sql)->execute();
		} catch (Exception $e) {
			$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
			echo "Error indexing ls_bill_vote_detail: $msg\n";
		}

		if ($db_engine == 'mysql')
		{
			echo "Committing changes\n";
			$db->commit();
		}
	} catch (Exception $e) {
		$msg = $e->getMessage() . ' in ' . basename($e->getFile()) . ' on line ' . $e->getLine();
		if ($db_engine == 'mysql')
		{
			$db->rollback();
			die("Rollback 1.4.0 upgrade: $msg\n");
		}
		else
		{
			die("ERROR: $msg\n");
		}
	}
}
// }}}

if (version_compare($version, $release_version) < 0)
{
	echo "Updated to version $release_version schema $release_schema\n";
	set_var('version', $release_version);
	set_var('schema', $release_schema);
}
else
{
	echo "Nothing to do, already at current release\n";
}
