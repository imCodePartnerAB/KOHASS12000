-- Tar bort den påhittade pågående körningen (jobbraden med status running och källa manual utan avslut).
DELETE FROM imcode_jobs WHERE status = 'running' AND requested_by = 'exempel-admin';
