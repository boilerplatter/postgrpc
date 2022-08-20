#!/bin/bash

set -e

# initialize the privilege-restricted application user
psql -U postgres -d postgres <<-EOSQL
  -- create non-superuser admin user
  CREATE USER admin WITH PASSWORD 'supersecretadminpassword' CREATEROLE NOSUPERUSER NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS;

  -- create app user with even fewer privileges
  CREATE USER appuser WITH PASSWORD 'supersecretpassword' NOCREATEROLE NOSUPERUSER NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS;

  -- create the application database owned by the admin
  CREATE DATABASE appdb OWNER admin;
EOSQL

# initialize limited privileges on the new database
psql -U postgres -d appdb <<-EOSQL
  -- make sure that non-superusers can't interact with functions and system tables
  REVOKE ALL ON SCHEMA pg_catalog FROM PUBLIC;
  REVOKE ALL ON SCHEMA information_schema FROM PUBLIC;
  ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

  -- manually revoke those functions not covered by the previous line
  DO \$\$
  DECLARE temp_proc RECORD;
  BEGIN FOR temp_proc IN
    SELECT proname, oidvectortypes(proargtypes)
    FROM pg_proc
    WHERE proname NOT ILIKE 'to_json%'
      AND proname NOT ILIKE 'nameeq%'
      AND proname NOT ILIKE 'uuid_eq%'
  LOOP
    BEGIN
      EXECUTE 'REVOKE EXECUTE ON FUNCTION ' || temp_proc.proname || '(' || temp_proc.oidvectortypes || ') FROM PUBLIC';
      EXCEPTION WHEN OTHERS THEN
    END;
  END LOOP;
  END \$\$;

  -- grant the app user the functions needed to recycle connections with the default pool
  GRANT EXECUTE ON FUNCTION pg_advisory_unlock_all() TO admin;
  GRANT EXECUTE ON FUNCTION pg_advisory_unlock_all() TO appuser;

  -- disallow additional PL/PgSQL
  DROP EXTENSION plpgsql;

  -- revoke everything but USAGE rights from appdb for new users
  REVOKE ALL ON SCHEMA public FROM PUBLIC;
  ALTER SCHEMA public OWNER TO admin;
  REVOKE CONNECT ON DATABASE appdb FROM PUBLIC;
  GRANT CONNECT ON DATABASE appdb TO appuser;
EOSQL

# set up the application database's schema as the admin
PGPASSWORD=supersecretadminpassword psql -U admin -d appdb <<-EOSQL
  -- enable relevant extensions
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  -- create a notes table for user-provided notes
  CREATE TABLE IF NOT EXISTS notes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    author NAME NOT NULL DEFAULT CURRENT_USER,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
  );

  -- make sure notes is guarded with row-level security policies
  ALTER TABLE notes ENABLE ROW LEVEL SECURITY;

  -- create row-level security policy allowing users to work with their own notes only
  CREATE POLICY author_notes_policy ON notes USING (author = CURRENT_USER);
EOSQL
