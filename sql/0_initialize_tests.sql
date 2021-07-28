-- different versions of postgres display different NOTICE messages,
-- so we prevent these messages from being displayed
SET client_min_messages TO WARNING;

-- load all of pgrollup's dependencies
CREATE LANGUAGE plpython3u;
CREATE EXTENSION datasketches;
CREATE EXTENSION hll;
CREATE EXTENSION pg_cron;
CREATE EXTENSION tdigest;
CREATE EXTENSION topn;
CREATE EXTENSION vector;

-- load pgrollup and configure it for automatic rollup creation to facilitate tests
CREATE EXTENSION pgrollup;
CREATE EVENT TRIGGER pgrollup_from_matview_trigger ON ddl_command_end WHEN TAG IN ('CREATE MATERIALIZED VIEW') EXECUTE PROCEDURE pgrollup_from_matview_event();
