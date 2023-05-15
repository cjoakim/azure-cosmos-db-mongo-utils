-- PostgreSQL DDL for the MMA report data
-- Chris Joakim, Microsoft, 2023

DROP TABLE IF EXISTS mma CASCADE;

CREATE TABLE mma (
    cluster                VARCHAR(255),
    database               VARCHAR(255),
    container              VARCHAR(255),
    container_count        INTEGER,
    sharded                VARCHAR(16),
    shard_key              VARCHAR(255),
    size_in_bytes          BIGINT,
    doc_count              BIGINT,
    avg_doc_size           NUMERIC(10, 3),
    largest                INTEGER,
    size_in_gb             NUMERIC(12, 3),
    pp_equiv               INTEGER,
    mpp                    VARCHAR(16),
    est_migration_ru       INTEGER,
    est_post_migration_ru  INTEGER,
    spacer                 VARCHAR(16),
    notes                  VARCHAR(255),
    features               VARCHAR(255),
    index_advice           VARCHAR(255),
    idx_warning            VARCHAR(16),
    idx_critical           VARCHAR(16),
    idx_score              VARCHAR(16),
    source_host            VARCHAR(255),
    cosmos_acct            VARCHAR(255)
);

\d mma
\copy mma(cluster,database,container,container_count,sharded,shard_key,size_in_bytes,doc_count,avg_doc_size,largest,size_in_gb,pp_equiv,mpp,est_migration_ru,est_post_migration_ru,spacer,notes,features,index_advice,idx_warning,idx_critical,idx_score,source_host,cosmos_acct) from current/psql/mma_report.csv csv header;
\x
select * from mma offset 0 limit 1;
select count(*) from mma;
