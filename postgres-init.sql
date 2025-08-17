-- PostgreSQL optimization script for HAPI FHIR bulk imports
-- This script optimizes the database for minimal disk usage and faster imports

-- Disable unnecessary extensions to save space
-- Note: Keep postgis as it might be needed by HAPI FHIR
-- DROP EXTENSION IF EXISTS postgis CASCADE;
-- DROP EXTENSION IF EXISTS postgis_topology CASCADE;

-- Set database-level optimizations
ALTER DATABASE postgres SET synchronous_commit = 'off';
ALTER DATABASE postgres SET wal_compression = 'on';
ALTER DATABASE postgres SET effective_cache_size = '3GB';
ALTER DATABASE postgres SET shared_buffers = '1GB';
ALTER DATABASE postgres SET work_mem = '32MB';
ALTER DATABASE postgres SET maintenance_work_mem = '256MB';
ALTER DATABASE postgres SET checkpoint_completion_target = 0.9;
ALTER DATABASE postgres SET random_page_cost = 1.1;
ALTER DATABASE postgres SET effective_io_concurrency = 200;

-- Create a function to disable indexes during bulk import
CREATE OR REPLACE FUNCTION disable_fhir_indexes()
RETURNS void AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Disable all non-primary key indexes for faster bulk imports
    FOR rec IN 
        SELECT schemaname, tablename, indexname 
        FROM pg_indexes 
        WHERE schemaname = 'public' 
        AND indexname NOT LIKE '%_pkey'
        AND tablename LIKE '%hfj_%'
    LOOP
        EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(rec.schemaname) || '.' || quote_ident(rec.indexname);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create a function to recreate indexes after bulk import
CREATE OR REPLACE FUNCTION enable_fhir_indexes()
RETURNS void AS $$
BEGIN
    -- This function would recreate the indexes
    -- You should run this after bulk import is complete
    RAISE NOTICE 'Indexes should be recreated by HAPI FHIR on restart or via reindex operations';
END;
$$ LANGUAGE plpgsql;

-- Create a function to vacuum and analyze tables after bulk import
CREATE OR REPLACE FUNCTION optimize_after_bulk_import()
RETURNS void AS $$
DECLARE
    rec RECORD;
BEGIN
    -- Vacuum and analyze all HAPI FHIR tables
    FOR rec IN 
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        AND tablename LIKE '%hfj_%'
    LOOP
        EXECUTE 'VACUUM ANALYZE ' || quote_ident(rec.schemaname) || '.' || quote_ident(rec.tablename);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Set up minimal logging for space savings
ALTER SYSTEM SET log_statement = 'none';
ALTER SYSTEM SET log_min_duration_statement = -1;
ALTER SYSTEM SET log_checkpoints = 'off';
ALTER SYSTEM SET log_connections = 'off';
ALTER SYSTEM SET log_disconnections = 'off';
ALTER SYSTEM SET log_lock_waits = 'off';
ALTER SYSTEM SET log_temp_files = -1;
ALTER SYSTEM SET log_autovacuum_min_duration = -1;

-- Reload configuration
SELECT pg_reload_conf();
