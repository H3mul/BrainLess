-- Disable transactions because database creation cannot run inside a block
\connect postgres

-- 1. Create the database
CREATE DATABASE $TIMESCALE_DB_NAME;

-- Re-connect to the newly created database to apply user permissions locally
\connect $TIMESCALE_DB_NAME

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. Create the Read/Write User
CREATE USER $TIMESCALE_DB_USER WITH PASSWORD '$TIMESCALE_DB_PASS';

-- Grant connection and schema creation privileges
GRANT CONNECT ON DATABASE $TIMESCALE_DB_NAME TO $TIMESCALE_DB_USER;
GRANT CREATE ON SCHEMA public TO $TIMESCALE_DB_USER;

-- Grant R/W permissions on all future tables/sequences created in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $TIMESCALE_DB_USER;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $TIMESCALE_DB_USER;


-- 3. Create the Read-Only User
CREATE USER $TIMESCALE_DB_READ_USER WITH PASSWORD '$TIMESCALE_DB_READ_USER_PASS';

-- Grant connection privileges only
GRANT CONNECT ON DATABASE $TIMESCALE_DB_NAME TO $TIMESCALE_DB_READ_USER;
GRANT USAGE ON SCHEMA public TO $TIMESCALE_DB_READ_USER;

-- Grant Read-Only permissions on all future tables created in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO $TIMESCALE_DB_READ_USER;
