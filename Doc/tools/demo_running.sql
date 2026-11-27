-- Lägger in en jobbrad med status "running" så att sidan visar en pågående körning (bild 13).
-- Ingenting körs på riktigt. Ta bort raden med demo_running_remove.sql.
INSERT INTO imcode_jobs (job_type, source, status, requested_by, requested_at, started_at, message)
VALUES ('import', 'manual', 'running', 'exempel-admin', NOW() - INTERVAL 4 MINUTE, NOW() - INTERVAL 3 MINUTE, NULL);
