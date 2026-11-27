-- Återställer pluginets tabeller till läget före demo_seed.sql (tomma tabeller, standardvärden).
-- Rör ENDAST pluginets egna tabeller (imcode_*).

UPDATE imcode_config SET value = 'your_client_id'    WHERE name = 'ist_client_id';
UPDATE imcode_config SET value = 'your_customerId'   WHERE name = 'ist_customer_id';
UPDATE imcode_config SET value = ''                  WHERE name IN ('koha_default_categorycode', 'koha_default_branchcode', 'civic_attribute_code');
UPDATE imcode_config SET value = 'userid,cardnumber' WHERE name = 'match_methods';
UPDATE imcode_config SET value = 'No'                WHERE name IN ('email_suffix_enabled', 'use_rules', 'staff_import');
UPDATE imcode_config SET value = 'off'               WHERE name = 'leaver_mode';

DELETE FROM imcode_branches_mapping;
DELETE FROM imcode_categories_mapping;
DELETE FROM imcode_rules;
DELETE FROM imcode_rule_log;
DELETE FROM imcode_schedule;
DELETE FROM imcode_jobs;
DELETE FROM imcode_logs;
DELETE FROM imcode_seen_values;
DELETE FROM imcode_state;
