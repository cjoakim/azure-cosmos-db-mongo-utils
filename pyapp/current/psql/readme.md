# readme for current/psql directory

This directory is used to store files relating to analysis and reporting
using PostgreSQL.  

## Sample Queries

### Data by Cluster, sorted by size in bytes

``` 
SELECT 
    cluster,
    sum(container_count) as container_count,
    sum(size_in_bytes) as size_in_bytes,
    sum(size_in_bytes) / 1099511627776 as size_in_tb,
    sum(doc_count) as doc_count
FROM mma
where container != 'Database Total'
group by cluster
order by size_in_bytes desc
```

### 

``` 
select cluster, database, container, source_host, cosmos_acct
from mma 
where container != 'Database Total'
and source_host = 'xxx'
and cosmos_acct = 'xxx'
```

``` 
select cluster, database, container, source_host, cosmos_acct
from mma 
where cluster = 'xxx'
```
