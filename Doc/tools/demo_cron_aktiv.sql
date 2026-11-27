-- Lägger in en puls så att statusrutan under "Schema och kör nu" visar "aktiv".
-- Ingenting körs i cron. Ta bort pulsen med demo_cron_saknas.sql.
INSERT INTO imcode_state (name, value) VALUES ('tick_last_seen', DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s'))
  ON DUPLICATE KEY UPDATE value = VALUES(value), updated_at = NOW();
