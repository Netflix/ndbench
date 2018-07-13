NDBench DAX Plugin Configuration
===================

NdBench configuration is backed by **Netflix OSS** - **Archaius-2** implementation. This
plugin allows you to drive loads to DAX (DynamoDB Accelerator) with NDBench.

## Clients
The `DaxKeyValue` client assumes that the named table exists, as well as an active DAX
cluster (endpoints available) that points to that table. `DaxKeyValue` builds on the client base class
and configuration in ndbench-dynamodb-plugins module.

## CLI invocation
Choose `DaxKeyValue` on
the command line with the `ndbench.config.cli.clientName` system property. All invocations below are
seven minute test runs, disable retries and set the request timeout to 100 milliseconds. Finally,
they assume that the CLI is being run on EC2 and that the EC2 instance has an instance profile IAM
role with permission to read and write data to DynamoDB tables. All examples below exercise the
default per-table maximum read and write capacity units in regions other than Virginia (us-east-1).
These runs assume each non-bulk API call will take 10ms and that the EC2 host you run on has the
resources to drive the load. If in doubt, monitor detailed CloudWatch metrics for the host while the
test is in progress (CPU Utilization, Network bytes in/out). These runs assume each bulk API call will
take 20ms to complete because the batch get/write APIs may paginate a few times.

### DaxKeyValue CLI invocation
The following minimal examples invoke DAX clients on the command line with minimal parameters.
These runs execute against an existing DAX cluster that is backing an existing table called `ndbench-table`.

#### DaxKeyValue CLI invocation with single reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the PutItem and
GetItem APIs on DAX.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DaxKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=10000 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=10000 \
-Dndbench.config.dynamodb.daxEndpoint=XXXXX.YYYYY.clustercfg.dax.use1.cache.amazonaws.com:8111 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=200 \
-Dndbench.config.dynamodb.readCapacityUnits=10000 \
-Dndbench.config.dynamodb.writeCapacityUnits=10000 \
-Dndbench.config.dynamodb.numReaders=100 \
-Dndbench.config.dynamodb.numWriters=100
```

#### DaxKeyValue CLI invocation with bulk reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the BatchWriteItem
and BatchGetItem APIs on DAX.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DaxKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.cli.bulkSize=25 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=400 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=400 \
-Dndbench.config.dynamodb.daxEndpoint=XXXXX.YYYYY.clustercfg.dax.use1.cache.amazonaws.com:8111 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=16 \
-Dndbench.config.dynamodb.readCapacityUnits=10000 \
-Dndbench.config.dynamodb.writeCapacityUnits=10000 \
-Dndbench.config.dynamodb.numReaders=8 \
-Dndbench.config.dynamodb.numWriters=8
```

## Invocation using the deployable WAR
Follow the instructions in the top-level readme and the wiki to start the web application, and select
the DAX client (`DaxKeyValue`).
from the pop-down list.

## Configuration details
The default configuration prefix is `ndbench.config.dax.`, as shown by the CLI examples above.
For more detailed information about parameters, see the interfaces in the package called
`com.netflix.ndbench.plugin.dax.configs`.
