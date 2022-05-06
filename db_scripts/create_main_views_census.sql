-- Contributor: Divya Maiya (dmaiya@umass.edu)

DROP VIEW IF EXISTS Married;
DROP VIEW IF EXISTS Unmarried;

CREATE VIEW Married AS (SELECT * FROM Census WHERE marital_status in (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated'));
CREATE VIEW Unmarried AS (SELECT * FROM Census WHERE marital_status in (' Never-married', ' Widowed',' Divorced'));

-- SELECT COUNT(*) FROM Married;
-- SELECT COUNT(*) FROM Unmarried;
