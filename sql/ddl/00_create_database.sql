-- KT KDAP PoC: Database and schema initialization
-- Run: impala-shell -k -f sql/ddl/00_create_database.sql

CREATE DATABASE IF NOT EXISTS kdap
  COMMENT 'KT KDAP PoC main database';

CREATE DATABASE IF NOT EXISTS kdap_staging
  COMMENT 'Staging area for distcp and CTAS';

CREATE DATABASE IF NOT EXISTS kdap_results
  COMMENT 'PoC benchmark output tables';

-- Grant to PoC service account (adjust principal)
-- GRANT ALL ON DATABASE kdap TO USER `kdap_svc@REALM`;

USE kdap;
