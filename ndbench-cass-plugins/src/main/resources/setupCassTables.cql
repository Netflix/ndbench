CREATE KEYSPACE IF NOT EXISTS dev1 WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3', 'dc3': '3'}  AND durable_writes = true;

//Astyanax Plugin

CREATE TABLE IF NOT EXISTS emp_thrift (key text, column1 int, value text, PRIMARY KEY ((key), column1)) WITH COMPACT STORAGE;

//JavaDriver Plugin
CREATE TABLE IF NOT EXISTS empl (emp_uname varchar primary key, emp_first varchar, emp_last varchar, emp_dept varchar);
