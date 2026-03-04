-- ============================================================================
-- METABASE SAVED QUESTIONS EXPORT — Run against metabase_v3 database
-- ============================================================================
-- Run these queries via your Metabase PostgreSQL client (pgAdmin, DBeaver, etc.)
-- while connected to VPN. Export results as CSV.
--
-- Database: metabase_v3
-- Host: production-postgress.cluster-cejra4hw3o2e.ap-south-1.rds.amazonaws.com
-- Port: 5432
-- User: postgres
-- ============================================================================

-- ============================================================================
-- QUERY 1: Saved native SQL questions (THE critical export for few-shot)
-- ============================================================================
-- This extracts all non-archived native SQL questions with their English names.
-- These become (question, SQL) pairs for few-shot retrieval in RAVEN.
-- Export as: metabase_native_questions.csv

SELECT
    rc.id AS question_id,
    rc.name AS question_name,
    rc.description,
    rc.query_type,
    rc.dataset_query::text AS dataset_query_json,
    -- Extract native SQL from the JSON (Metabase stores it as JSON)
    (rc.dataset_query::json ->> 'native')::json ->> 'query' AS sql_query,
    -- Extract the database ID to know which DB the query targets
    (rc.dataset_query::json ->> 'database')::int AS target_database_id,
    rc.display AS visualization_type,
    rc.collection_id,
    c.name AS collection_name,
    c.slug AS collection_slug,
    cu.email AS creator_email,
    rc.created_at,
    rc.updated_at,
    rc.result_metadata::text AS result_metadata_json,
    rc.cache_ttl,
    rc.archived
FROM report_card rc
LEFT JOIN collection c ON rc.collection_id = c.id
LEFT JOIN core_user cu ON rc.creator_id = cu.id
WHERE rc.query_type = 'native'
  AND rc.archived = false
  AND (rc.dataset_query::json ->> 'database')::int = 300
ORDER BY rc.updated_at DESC;


-- ============================================================================
-- QUERY 2: GUI-based questions (structured queries — backup/reference)
-- ============================================================================
-- These are "simple" questions built via Metabase's GUI query builder.
-- Metabase stores them as structured JSON, not raw SQL.
-- Less useful for few-shot but shows what analysts are querying.
-- Export as: metabase_gui_questions.csv

SELECT
    rc.id AS question_id,
    rc.name AS question_name,
    rc.description,
    rc.query_type,
    rc.dataset_query::text AS dataset_query_json,
    rc.display AS visualization_type,
    (rc.dataset_query::json ->> 'database')::int AS target_database_id,
    rc.collection_id,
    c.name AS collection_name,
    cu.email AS creator_email,
    rc.created_at,
    rc.updated_at,
    rc.archived
FROM report_card rc
LEFT JOIN collection c ON rc.collection_id = c.id
LEFT JOIN core_user cu ON rc.creator_id = cu.id
WHERE rc.query_type = 'query'
  AND rc.archived = false
  AND (rc.dataset_query::json ->> 'database')::int = 300
ORDER BY rc.updated_at DESC;


-- ============================================================================
-- QUERY 3: Database connections configured in Metabase
-- ============================================================================
-- This tells us which database IDs map to which actual databases
-- (e.g., which ID = your Trino/cdp connection).
-- Export as: metabase_databases.csv

SELECT
    id AS database_id,
    name AS database_name,
    engine,
    created_at,
    updated_at,
    is_audit,
    is_sample
FROM metabase_database
ORDER BY id;


-- ============================================================================
-- QUERY 4: Table metadata from Metabase (what Metabase knows about tables)
-- ============================================================================
-- Metabase syncs table/column metadata. This shows what it discovered.
-- Export as: metabase_tables.csv

SELECT
    mt.id AS table_id,
    mt.db_id AS database_id,
    md.name AS database_name,
    mt.schema AS table_schema,
    mt.name AS table_name,
    mt.display_name,
    mt.description AS table_description,
    mt.rows AS estimated_rows,
    mt.active,
    mt.visibility_type,
    mt.created_at,
    mt.updated_at
FROM metabase_table mt
JOIN metabase_database md ON mt.db_id = md.id
WHERE mt.active = true
  AND mt.db_id = 300
ORDER BY mt.schema, mt.name;


-- ============================================================================
-- QUERY 5: Dashboards (for understanding common analysis patterns)
-- ============================================================================
-- Export as: metabase_dashboards.csv

SELECT
    rd.id AS dashboard_id,
    rd.name AS dashboard_name,
    rd.description,
    rd.collection_id,
    c.name AS collection_name,
    cu.email AS creator_email,
    rd.created_at,
    rd.updated_at,
    rd.archived,
    -- Count of cards on this dashboard
    (SELECT COUNT(*) FROM report_dashboardcard rdc WHERE rdc.dashboard_id = rd.id) AS card_count
FROM report_dashboard rd
LEFT JOIN collection c ON rd.collection_id = c.id
LEFT JOIN core_user cu ON rd.creator_id = cu.id
WHERE rd.archived = false
ORDER BY rd.updated_at DESC;


-- ============================================================================
-- QUERY 6: Dashboard → Question mappings (which questions are on which dashboards)
-- ============================================================================
-- Export as: metabase_dashboard_cards.csv

SELECT
    rdc.id AS dashboardcard_id,
    rdc.dashboard_id,
    rd.name AS dashboard_name,
    rdc.card_id AS question_id,
    rc.name AS question_name,
    rc.query_type,
    rdc.row AS grid_row,
    rdc.col AS grid_col,
    rdc.size_x,
    rdc.size_y,
    rdc.created_at
FROM report_dashboardcard rdc
JOIN report_dashboard rd ON rdc.dashboard_id = rd.id
LEFT JOIN report_card rc ON rdc.card_id = rc.id
WHERE rd.archived = false
ORDER BY rdc.dashboard_id, rdc.row, rdc.col;


-- ============================================================================
-- QUERY 7: Collections (folder structure)
-- ============================================================================
-- Export as: metabase_collections.csv

SELECT
    id AS collection_id,
    name AS collection_name,
    slug,
    description,
    location,
    personal_owner_id,
    archived,
    created_at
FROM collection
WHERE archived = false
ORDER BY location, name;


-- ============================================================================
-- QUERY 8: Quick stats (run this first to see what's available)
-- ============================================================================
-- Don't export — just run to see counts

SELECT 'native_questions_db300' AS metric, COUNT(*) AS cnt 
FROM report_card WHERE query_type = 'native' AND archived = false AND (dataset_query::json ->> 'database')::int = 300
UNION ALL
SELECT 'gui_questions_db300', COUNT(*) 
FROM report_card WHERE query_type = 'query' AND archived = false AND (dataset_query::json ->> 'database')::int = 300
UNION ALL
SELECT 'total_questions_db300', COUNT(*) 
FROM report_card WHERE archived = false AND (dataset_query::json ->> 'database')::int = 300
UNION ALL
SELECT 'active_tables_db300', COUNT(*) 
FROM metabase_table WHERE active = true AND db_id = 300
UNION ALL
SELECT 'database_300_name', 0 
FROM metabase_database WHERE id = 300
UNION ALL
SELECT 'dashboards_total', COUNT(*) 
FROM report_dashboard WHERE archived = false
UNION ALL
SELECT 'collections_total', COUNT(*) 
FROM collection WHERE archived = false;
