NDBench DynamoDB Plugin Configuration
===================

NdBench configuration is backed by **Netflix OSS** - **Archaius-2** implementation. This
plugin allows you to drive loads to DynamoDB with NDBench.

## Clients
The NDBench DynamoDB plugin offers a few NdBenchClient implementations to choose from.
- `DynamoDBKeyValue`: This client assumes that the table named in the configuration
property `ndbench.config.dynamodb.tableName` already exists. Unless configured with the
table a-priori, this client will not configure autoscaling, high resolution metrics and
alarms.
- `DynamoDBProgrammaticKeyValue`: This client tries to create the table named in configuration
if it does not exist. Also, it will try to enable autoscaling, high resolution metrics and high
resolution alarms for capacity consumption.
- `DaxKeyValue`: This client assumes that the named table exists, as well as an active DAX
cluster (endpoints available) that points to that table.

## CLI invocation
You can choose between `DynamoDBKeyValue`, `DynamoDBProgrammaticKeyValue`, and `DaxKeyValue` on
the command line with the `ndbench.config.cli.clientName` system property. All invocations below are
seven minute test runs, disable retries and set the request timeout to 100 milliseconds. Finally,
they assume that the CLI is being run on EC2 and that the EC2 instance has an instance profile IAM
role with permission to read and write data to DynamoDB tables. All examples below exercise the
default per-table maximum read and write capacity units in regions other than Virginia (us-east-1).
These runs assume each non-bulk API call will take 10ms and that the EC2 host you run on has the
resources to drive the load. If in doubt, monitor detailed CloudWatch metrics for the host while the
test is in progress (CPU Utilization, Network bytes in/out). These runs assume each bulk API call will
take 20ms to complete because the batch get/write APIs may paginate a few times.

### DynamoDBKeyValue CLI invocation
The following minimal examples invoke DynamoDB clients on the command line with minimal parameters.
These runs assume a table called "ndbench-table" provisioned at 10,000 reads and 10,000 writes already
exists.

#### DynamoDBKeyValue CLI invocation with single reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the PutItem and
GetItem APIs.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DynamoDBKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=10000 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=10000 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=200 \
-Dndbench.config.dynamodb.numReaders=100 \
-Dndbench.config.dynamodb.numWriters=100
```

#### DynamoDBKeyValue CLI invocation with bulk reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the BatchWriteItem
and BatchGetItem APIs.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DynamoDBKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.cli.bulkSize=25 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=400 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=400 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=16 \
-Dndbench.config.dynamodb.numReaders=8 \
-Dndbench.config.dynamodb.numWriters=8
```

### DynamoDBProgrammaticKeyValue CLI invocation
The following minimal examples invoke DynamoDB clients on the command line with minimal parameters.
These runs will create and delete a table called "ndbench-table" with 20,000 reads and 20,000 writes
provisioned. The benchmark will delete the tables at the end of the run.

#### DynamoDBProgrammaticKeyValue CLI invocation with single reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the PutItem and
GetItem APIs.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DynamoDBProgrammaticKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=10000 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=10000 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=200 \
-Dndbench.config.dynamodb.readCapacityUnits=10000 \
-Dndbench.config.dynamodb.writeCapacityUnits=10000 \
-Dndbench.config.dynamodb.numReaders=100 \
-Dndbench.config.dynamodb.numWriters=100
```

#### DynamoDBKeyValue CLI invocation with bulk reads and writes
This example drives 10,000 reads and 10,000 writes per second to the default table using the BatchWriteItem
and BatchGetItem APIs.

```bash
DISCOVERY_ENV=AWS ./gradlew run \
-Dndbench.config.cli.clientName=DynamoDBProgrammaticKeyValue \
-Dndbench.config.cli.timeoutMillis=420000 \
-Dndbench.config.dataSize=1000 \
-Dndbench.config.cli.bulkSize=25 \
-Dndbench.config.numKeys=10000000 \
-Dndbench.config.readEnabled=true \
-Dndbench.config.readRateLimit=400 \
-Dndbench.config.writeEnabled=true \
-Dndbench.config.writeRateLimit=400 \
-Dndbench.config.dynamodb.maxRetries=0 \
-Dndbench.config.dynamodb.requestTimeout=100 \
-Dndbench.config.dynamodb.maxConnections=16 \
-Dndbench.config.dynamodb.readCapacityUnits=10000 \
-Dndbench.config.dynamodb.writeCapacityUnits=10000 \
-Dndbench.config.dynamodb.numReaders=8 \
-Dndbench.config.dynamodb.numWriters=8
```

### DaxKeyValue CLI invocation
The following minimal examples invoke DynamoDB clients on the command line with minimal parameters.
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
one of the DynamoDB clients (`DynamoDBKeyValue`, `DynamoDBProgrammaticKeyValue`, and `DaxKeyValue`)
from the pop-down list.

## Configuration details
The default configuration prefix is `ndbench.config.dynamodb.`, as shown by the CLI examples above.
For more detailed information about parameters, see the interfaces in the package called
`com.netflix.ndbench.plugin.dynamodb.configs`.
