-- Create 3-layer schemas

-- Layer 1: Staging (Raw data)
CREATE SCHEMA staging;
COMMENT ON SCHEMA staging IS 'Layer 1: Raw data from sources';

-- Layer 2: Reconciled (Clean, integrated)
CREATE SCHEMA reconciled;
COMMENT ON SCHEMA reconciled IS 'Layer 2: Reconciled and integrated data';

-- Layer 3: Data Warehouse (Star schema)
CREATE SCHEMA dwh;
COMMENT ON SCHEMA dwh IS 'Layer 3: Dimensional model for analytics';

-- Verify schemas
SELECT schema_name, 
       CASE schema_name 
         WHEN 'staging' THEN 'Layer 1: Raw Data'
         WHEN 'reconciled' THEN 'Layer 2: Reconciled'
         WHEN 'dwh' THEN 'Layer 3: Star Schema'
       END as layer_description
FROM information_schema.schemata 
WHERE schema_name IN ('staging', 'reconciled', 'dwh');