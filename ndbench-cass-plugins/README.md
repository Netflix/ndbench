NDBench Cassandra Plugins Configuration
===================

NdBench configuration is backed by **Netflix OSS** - **Archaius-2** implementation

----------


Default Configuration prefix - `ndbench.config.`



> **Note:**
Every config has to be prefixed with configuration prefix mentioned above


#### Cassandra Plugins Common configurations
| Config Name     | Default value | Description   |
| :------- | :---- | :--- |
|cass.cluster| localhost | Destination C* Cluster Name |
|cass.host| 127.0.0.1 | IP of one of the C* nodes
|cass.host.port| 9042 | PORT of one of the C* nodes
|cass.username| null | Username of the C* nodes
|cass.password| null | Password of the C* nodes
|cass.keyspace| 	dev1 | Destination Keyspace Name|
|cass.cfname| 	emp | Destination CF Name|
|cass.readConsistencyLevel| 	LOCAL_ONE | Read CL|
|cass.writeConsistencyLevel| 	LOCAL_ONE | Write CL|
|cass.truststorePath| null | Absolute path of truststore
|cass.truststorePass| null | Password of truststore


#### Cassandra *CassJavaDriverGeneric* Plugin configurations
| Config Name     | Default value | Description   |
| :------- | :---- | :--- |
|cass.colsPerRow| 100 | Number of columns per row|

#### Cassandra *CassJavaDriverBatch* Plugin configurations

| Config Name     | Default value | Description   |
| :------- | :---- | :--- |
|cfname2| test2| Seconds CF name to be used in `useMultiPartition` |
|batchSize| 3| Number of individual inserts to be in Batch|
|useTimestamp| true| Indicates whether to use client side timestamp for batch or not |
|useMultiPartition| false| Indicated whether to insert into multiple tables with in the same batch or not|


#### Cassandra *CassJavaDriverPlugin* Plugin configurations
| Config Name     | Default value | Description   |
| :------- | :---- | :--- |
|cass.cluster| localhost | Destination C* Cluster Name |
\|cass.cfname| empl | Destination CF Name|

> **Note:**
Definitions of C* keyspaces and tables used in these plugins are available @ setupCassTables.cql (https://github.com/Netflix/ndbench/blob/master/ndbench-cass-plugins/src/main/resources/setupCassTables.cql)
