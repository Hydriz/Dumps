-- Tables for the Wikidata dumps module

CREATE TABLE wikidata (
    wiki VARCHAR(255),
    dumpdate DATE,
    claimed_by VARCHAR(255),
    can_archive INT,
    is_archived INT,
    is_checked INT,
    comments VARCHAR(255)
);

CREATE INDEX is_archived ON wikidata (is_archived);
CREATE INDEX is_checked ON wikidata (is_checked);
