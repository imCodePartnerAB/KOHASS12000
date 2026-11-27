-- Tar bort exempellåntagaren Anna Exempel (userid anna.exempel) och hennes attribut och loggrader.
DELETE FROM borrower_attributes WHERE borrowernumber IN (SELECT borrowernumber FROM borrowers WHERE userid = 'anna.exempel');
DELETE FROM imcode_data_change_log WHERE table_name = 'borrowers' AND record_id IN (SELECT borrowernumber FROM borrowers WHERE userid = 'anna.exempel');
DELETE FROM borrowers WHERE userid = 'anna.exempel';
