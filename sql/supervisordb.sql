\c postgres

-- The DB name bdr_supervisordb is reserved by BDR. None
-- of these commands may be permitted.

CREATE DATABASE bdr_supervisordb;

DROP DATABASE bdr_supervisordb;

ALTER DATABASE bdr_supervisordb RENAME TO someothername;

ALTER DATABASE regression RENAME TO bdr_supervisordb;

-- Connections to the supervisordb are also prevented, but
-- cannot be tested here because of #63 at this point.
