-- Tables for Balchivist SQL backend

CREATE TABLE dumps (
	wiki VARCHAR(255),
	dumpdate DATE,
	progress VARCHAR(255),
	claimed_by VARCHAR(255),
	can_archive INT,
	is_archived INT,
	is_checked INT,
	comments VARCHAR(255)
);

CREATE INDEX progress ON dumps (progress);
CREATE INDEX is_archived ON dumps (is_archived);
CREATE INDEX is_checked ON dumps (is_checked);
