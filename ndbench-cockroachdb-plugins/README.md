NDBench Cockroach DB Plugins Configuration
===================

NdBench configuration is backed by **Netflix OSS** - **Archaius-2** implementation

----------


Default Configuration prefix - `ndbench.config.`



> **Note:**
Every config has to be prefixed with configuration prefix mentioned above


#### CockroachDB Plugins Common configurations
| Config Name     | Default value | Description   |
| :------- | :---- | :--- |
|cockroachdb.dbname| perftest | Destination CockroachDB Database Name |
|cockroachdb.tablename| test | Destination CockroachDB Table Name |
|cockroachdb.loadbalancer| test-loadbalancer | Cockroach DB Load balancer |
|cockroachdb.user| maxroach | Cockroach DB User |


## CockroachDB Plugins

### CockroachDBSimplePlugin
Performs simple read/write pattern for key, column, value.

### CockroachDBTransactionPlugin
Performs transactions involving parent table and child tables.

### CockroachDBSecondaryIndexPlugin
Performs writes involving columns that have secondary indices.
Can be enhanced to do reads using secondary indices.

