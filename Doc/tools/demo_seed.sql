-- Exempeldata för skärmbilderna i manualen. Bara påhittade värden.
-- Rör ENDAST pluginets egna tabeller (imcode_*). Inga låntagare, kategorier eller bibliotek ändras.
-- Förutsätter att biblioteken BECK och BRED och kategorierna BARN, UNG och PERSONAL finns i Koha
-- (standardkategorier och bibliotek i en vanlig Koha-demo). Skriptet kan köras om: det ger alltid samma resultat.
-- Återställ med demo_reset.sql.

-- 1. Inställningar (raderna finns redan; bara värden ändras)
UPDATE imcode_config SET value = 'exempel-klient'  WHERE name = 'ist_client_id';
UPDATE imcode_config SET value = 'EXEMPEL01'       WHERE name = 'ist_customer_id';
UPDATE imcode_config SET value = 'UNG'             WHERE name = 'koha_default_categorycode';
UPDATE imcode_config SET value = 'BECK'            WHERE name = 'koha_default_branchcode';
UPDATE imcode_config SET value = 'PNR'             WHERE name = 'civic_attribute_code';
UPDATE imcode_config SET value = 'attribute,userid,cardnumber' WHERE name = 'match_methods';
UPDATE imcode_config SET value = 'Yes'             WHERE name = 'email_suffix_enabled';
UPDATE imcode_config SET value = 'Yes'             WHERE name = 'use_rules';
UPDATE imcode_config SET value = 'Yes'             WHERE name = 'staff_import';
UPDATE imcode_config SET value = 'report'          WHERE name = 'leaver_mode';

-- 2. Skolmappning (tre påhittade skolor)
DELETE FROM imcode_branches_mapping;
INSERT INTO imcode_branches_mapping (organisationCode, branchcode) VALUES
  ('SCHOOL1', 'BECK'), ('SCHOOL2', 'BECK'), ('SCHOOL3', 'BRED');

-- 3. Rollmappning (äldre läge)
DELETE FROM imcode_categories_mapping;
INSERT INTO imcode_categories_mapping (categorycode, dutyRole, not_import) VALUES
  ('PERSONAL', 'Bibliotekarie', NULL), ('PERSONAL', 'Lärare', 1);

-- 4. Kategoriregler
DELETE FROM imcode_rules;
ALTER TABLE imcode_rules AUTO_INCREMENT = 1;
INSERT INTO imcode_rules (priority, name, enabled, scope, conditions_json, action_category, action_branch, not_import) VALUES
  (1, 'Exempel: årskurs 1-3', 1, 'category', '[{"field":"school_year","op":"in","values":["1","2","3"]}]', 'BARN', NULL, 0),
  (2, 'Exempel: bibliotekarie', 1, 'category', '[{"field":"role","op":"in","values":["Bibliotekarie"]}]', 'PERSONAL', NULL, 0),
  (3, 'Exempel: lärare', 1, 'category', '[{"field":"role","op":"in","values":["Lärare"]}]', NULL, NULL, 1);

-- 5. Schema
DELETE FROM imcode_schedule;
INSERT INTO imcode_schedule (weekday, run_time, job_type) VALUES
  (1, '08:00', 'import'), (2, '20:00', 'import'), (6, '13:00', 'import');

-- 6. Jobb (historik)
DELETE FROM imcode_jobs;
ALTER TABLE imcode_jobs AUTO_INCREMENT = 1;
INSERT INTO imcode_jobs (job_type, source, schedule_key, status, requested_by, requested_at, started_at, finished_at, message) VALUES
  ('import',   'schedule', '1-08:00-import:2026-11-23', 'done',   'schedule',      '2026-11-23 08:00:03', '2026-11-23 08:01:02', '2026-11-23 08:14:40', NULL),
  ('discover', 'manual',   NULL,                         'done',   'exempel-admin', '2026-11-24 10:12:00', '2026-11-24 10:13:01', '2026-11-24 10:17:30', 'stored 120 values, 600 persons read'),
  ('import',   'schedule', '2-20:00-import:2026-11-24', 'failed', 'schedule',      '2026-11-24 20:00:02', '2026-11-24 20:01:01', '2026-11-25 04:01:30', 'time-limit-8h'),
  ('import',   'manual',   NULL,                         'done',   'exempel-admin', '2026-11-26 09:30:00', '2026-11-26 09:31:00', '2026-11-26 09:44:12', NULL);

-- 7. Importlogg (en rad per sida, påhittade antal)
DELETE FROM imcode_logs;
ALTER TABLE imcode_logs AUTO_INCREMENT = 1;
INSERT INTO imcode_logs (page_token_next, response, record_count, is_processed, data_endpoint, data_hash, added_count, updated_count, processed_count, iteration_number, organisation_code, created_at) VALUES
  ('token-exempel-1', 'Added: 4, Updated: 26, Total: 30', 30, 1, 'persons', 'exempel-hash-01', 4, 26, 30, 1, 'SCHOOL1', NOW() - INTERVAL 3 HOUR),
  (NULL, 'Processing completed for SCHOOL1. Organization statistics, total processed: 52', 30, 1, 'persons', 'exempel-hash-02', 1, 21, 22, 2, 'SCHOOL1', NOW() - INTERVAL 170 MINUTE),
  (NULL, 'Processing completed for SCHOOL2. Organization statistics, total processed: 18', 30, 1, 'persons', 'exempel-hash-03', 0, 18, 18, 1, 'SCHOOL2', NOW() - INTERVAL 160 MINUTE),
  (NULL, 'Organisation code SCHOOL3 does not exist in IST - skipped for today (check the branch mapping)', NULL, 1, 'persons', 'exempel-hash-04', 0, 0, 0, 1, 'SCHOOL3', NOW() - INTERVAL 150 MINUTE),
  (NULL, 'Restart 1 of 10 from first page: NEW_DATA_RESTART_FROM_FIRST_PAGE', NULL, 1, 'restart_marker', 'exempel-hash-05', 0, 0, 0, 0, 'SCHOOL2', NOW() - INTERVAL 165 MINUTE);

-- 8. Värden hämtade från IST, och status för minutjobbet och elever som slutat
DELETE FROM imcode_seen_values;
INSERT INTO imcode_seen_values (field_name, value, label, seen_count, last_seen) VALUES
  ('school', 'SCHOOL1', NULL, 1, NOW()), ('school', 'SCHOOL2', NULL, 1, NOW()), ('school', 'SCHOOL3', NULL, 1, NOW()),
  ('school_year', '1', NULL, 20, NOW()), ('school_year', '2', NULL, 22, NOW()), ('school_year', '3', NULL, 18, NOW()),
  ('class', 'PRG25', NULL, 24, NOW()), ('role', 'Bibliotekarie', NULL, 2, NOW()), ('role', 'Lärare', NULL, 14, NOW());
DELETE FROM imcode_state WHERE name IN ('tick_last_seen', 'leavers_last');
INSERT INTO imcode_state (name, value) VALUES ('leavers_last', '2026-11-26 10:05 report: would_move 2, keep 1');
-- Minutjobbet: kör demo_cron_aktiv.sql för statusrutan "aktiv" (annars visas "saknas").
