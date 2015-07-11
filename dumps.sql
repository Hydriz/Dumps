-- Tables for Balchivist SQL backend

CREATE TABLE archive (
	type VARCHAR(255),
	subject VARCHAR(255),
	dumpdate DATE,
	progress VARCHAR(255),
	claimed_by VARCHAR(255),
	can_archive INT,
	is_archived INT,
	is_checked INT,
	comments VARCHAR(255)
);

CREATE INDEX progress ON archive (progress);
CREATE INDEX is_archived ON archive (is_archived);
CREATE INDEX is_checked ON archive (is_checked);
