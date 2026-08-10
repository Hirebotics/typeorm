-- Hirebotics / Beacon — NOT part of upstream TypeORM.
-- Extensions the TypeORM test-suite entities commonly expect.
-- Runs automatically the first time the container's data volume is created (see beacon/docker-compose.yml).
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";