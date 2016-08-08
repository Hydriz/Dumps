-- Patch for migrating the old "archive" table to the new "dumps" table and
-- changing the "subject" column to "wiki" for clarity.
-- The "type" column is also dropped as it is no longer needed.
-- Used for upgrading from version 1.1.0.

ALTER TABLE archive
    RENAME TO dumps,
    DROP COLUMN type,
    CHANGE COLUMN subject wiki VARCHAR(255);
