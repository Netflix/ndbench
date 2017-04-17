CREATE KEYSPACE IF NOT EXISTS dev1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3', 'dc3': '3'}  AND durable_writes = true;

//CassAstyanaxPlugin Plugin

CREATE TABLE IF NOT EXISTS dev1.emp_thrift (key text, column1 int, value text, PRIMARY KEY ((key), column1)) WITH COMPACT STORAGE;

//CassJavaDriverPlugin Plugin
CREATE TABLE IF NOT EXISTS dev1.empl (emp_uname varchar primary key, emp_first varchar, emp_last varchar, emp_dept varchar);

//CassJavaDriverGeneric Plugin
CREATE TABLE IF NOT EXISTS dev1.emp (key text, column1 int, value text, PRIMARY KEY (key, column1)) WITH CLUSTERING ORDER BY (column1 ASC)

//CassJavaDriverBatch Plugin
CREATE TABLE IF NOT EXISTS dev1.emp (cyclist_name text, expense_id int, amount float, balance float static, description text, paid boolean, PRIMARY KEY (cyclist_name, expense_id)) WITH CLUSTERING ORDER BY (expense_id ASC)
CREATE TABLE IF NOT EXISTS dev1.test2 (expense_id int, cyclist_name text, PRIMARY KEY (expense_id, cyclist_name)) WITH CLUSTERING ORDER BY (cyclist_name ASC)