#!/bin/bash

set -e

# initialize the privilege-restricted application user
psql -U postgres -d postgres <<-EOSQL
  -- create scoped and unprivileged database + user combo
  CREATE USER appuser WITH PASSWORD 'supersecretpassword' CREATEROLE NOSUPERUSER NOCREATEDB NOINHERIT NOREPLICATION NOBYPASSRLS;
  CREATE DATABASE appdb OWNER appuser;

  -- revoke everything but USAGE rights from appdb for new users
  REVOKE ALL ON SCHEMA public FROM PUBLIC;
  ALTER SCHEMA public OWNER TO appuser;
  REVOKE CONNECT ON DATABASE appdb FROM PUBLIC;
EOSQL

# set up the application database's schema
PGPASSWORD=supersecretpassword psql -U appuser -d appdb <<-EOSQL
  -- enable relevant extensions
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  -- create a notes table for user-provided notes
  CREATE TABLE IF NOT EXISTS notes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    author NAME NOT NULL DEFAULT CURRENT_USER,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
  );

  -- create row-level security policy allowing users to work with their own notes only
  CREATE POLICY author_notes_policy ON notes USING (author = CURRENT_USER);
EOSQL
