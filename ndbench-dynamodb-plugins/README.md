NDBench DynamDB Plugins Configuration
===================

NdBench configuration is backed by **Netflix OSS** - **Archaius-2** implementation

----------


Default Configuration prefix - `ndbench.config.dynamodb.`



> **Note:**
Every config has to be prefixed with configuration prefix mentioned above

### Table Creation

There are two ways to create DynamoDB Tables with NDBench. The first is by creating the DynamoDB Tables from the AWS Console.
The second is to allow NDBench to create the tables programmatically. This is configured by the following configuration
`ndbench.config.dynamodb.isProgramTables` (defaults to `false`). False means that tables will be configured by the AWS console.

#### AWS Console Configuration

In the console, you can define primary key, range key, capacity units and so forth. The following configurations at NDBench
must match the configurations in the console

   *  `ndbench.config.dynamodb.tablename`: is the table name used for DynamoDB table. Defaults to `ndbench-table`
   *  `ndbench.config.dynamodb.attributename`: is the name of the primary key
   
#### Programmatic Tables Configuration

Apart from the above properties, if you would like NDBench to create the table, then you can configure the following additional paramters

   *  `ndbench.config.dynamodb.rcu`: Table Read Capacity Units
   *  `ndbench.config.dynamodb.wcu`: Table Write Capacity Units
   
### Request Configuration

DynamoDB supports [eventually consistent and strongly consistent reads](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html). The following property configures the consistency level of the reads `ndbench.config.dynamodb.consistentread`.

### Amazon DynamoDB Accelerator (DAX)

You can configure the properties for DAX. The following two properties can be used
    * `ndbench.config.dax`: Enabling/Disabling the use of DAX. Defaults to `false`
    * `ndbench.config.daxEndpoint`: Adding the DAX endpoint. It can be found in the AWS console.



