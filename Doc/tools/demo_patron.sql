-- En påhittad importerad låntagare för bild 19. Personnumret har dag 99 och kan aldrig vara riktigt.
-- Tas bort med demo_patron_remove.sql. Rör bara denna låntagare och dess attribut.
INSERT INTO borrowers (cardnumber, surname, firstname, preferred_name, dateofbirth, categorycode, branchcode,
                       address, city, zipcode, email, userid, dateenrolled, dateexpiry, opacnote, sex)
SELECT '20150199-1234', 'Exempel', 'Anna', 'Anna', '2015-01-31', 'BARN', 'BECK',
       'Exempelgatan 1', 'Exempelstad', '12345', 'anna.exempel@example.invalid.NOEMAIL', 'anna.exempel',
       CURDATE(), '2027-06-30',
       'Added by SS12000: plugin 20261119 v2.0 at 2026-11-26 09:32:10', 'F'
FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM borrowers WHERE userid = 'anna.exempel');

INSERT INTO borrower_attributes (borrowernumber, code, attribute)
SELECT b.borrowernumber, 'CL', 'PRG25' FROM borrowers b
WHERE b.userid = 'anna.exempel' AND NOT EXISTS (SELECT 1 FROM borrower_attributes a WHERE a.borrowernumber = b.borrowernumber AND a.code = 'CL');
INSERT INTO borrower_attributes (borrowernumber, code, attribute)
SELECT b.borrowernumber, 'PNR', '201501991234' FROM borrowers b
WHERE b.userid = 'anna.exempel' AND NOT EXISTS (SELECT 1 FROM borrower_attributes a WHERE a.borrowernumber = b.borrowernumber AND a.code = 'PNR');
