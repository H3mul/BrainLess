CREATE EXTENSION IF NOT EXISTS timescaledb;

DO $$
BEGIN
    CREATE USER {{ app_user | identifier }} WITH PASSWORD {{ app_password | literal }};
EXCEPTION
    WHEN duplicate_object THEN
        ALTER USER {{ app_user | identifier }} WITH PASSWORD {{ app_password | literal }};
END $$;

GRANT CONNECT ON DATABASE {{ database | identifier }} TO {{ app_user | identifier }};
GRANT CREATE ON SCHEMA public TO {{ app_user | identifier }};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {{ app_user | identifier }};
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {{ app_user | identifier }};

{% if provision_read_user %}
DO $$
BEGIN
    CREATE USER {{ read_user | identifier }} WITH PASSWORD {{ read_password | literal }};
EXCEPTION
    WHEN duplicate_object THEN
        ALTER USER {{ read_user | identifier }} WITH PASSWORD {{ read_password | literal }};
END $$;

GRANT CONNECT ON DATABASE {{ database | identifier }} TO {{ read_user | identifier }};
GRANT USAGE ON SCHEMA public TO {{ read_user | identifier }};
GRANT SELECT ON ALL TABLES IN SCHEMA public TO {{ read_user | identifier }};
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO {{ read_user | identifier }};
ALTER DEFAULT PRIVILEGES FOR ROLE {{ app_user | identifier }} IN SCHEMA public GRANT SELECT ON TABLES TO {{ read_user | identifier }};
ALTER DEFAULT PRIVILEGES FOR ROLE {{ app_user | identifier }} IN SCHEMA public GRANT SELECT ON SEQUENCES TO {{ read_user | identifier }};
{% endif %}
