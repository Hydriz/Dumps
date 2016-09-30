-- Tables for the Wikimedia media files visit counts (mediacounts) module

CREATE TABLE mediacounts (
    dumpdate DATE,
    claimed_by VARCHAR(255),
    can_archive INT,
    is_archived INT,
    is_checked INT,
    comments VARCHAR(255)
);

CREATE INDEX is_archived ON mediacounts (is_archived);
CREATE INDEX is_checked ON mediacounts (is_checked);
