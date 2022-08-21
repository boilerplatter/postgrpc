#!/bin/bash

set -e

# populate the application database with some test data
PGPASSWORD=supersecretpassword psql -U postgres -d postgres <<-EOSQL
  -- enable relevant extensions
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

  -- create a notes table
  CREATE TABLE IF NOT EXISTS notes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
  );

  -- insert a couple of notes
  INSERT INTO notes (note) VALUES ('first note');
  INSERT INTO notes (note) VALUES ('second note');
  INSERT INTO notes (note) VALUES ('third note');
EOSQL
