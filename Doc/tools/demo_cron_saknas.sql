-- Tar bort pulsen så att statusrutan visar att minutjobbet aldrig setts.
DELETE FROM imcode_state WHERE name = 'tick_last_seen';
