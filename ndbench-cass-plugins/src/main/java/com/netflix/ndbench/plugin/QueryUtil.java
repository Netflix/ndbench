package com.netflix.ndbench.plugin;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryUtil {
    public static final String INSERT_QUERY = "INSERT INTO %s.%s (key, column1 , %s ) VALUES (?, ?, %s )";
    public static final String READ_QUERY = "SELECT * FROM %s.%s WHERE key = ?";

    public static String upsertCFQuery(Integer colsPerRow, String keyspaceName, String tableName) {
        String createTblQuery = "CREATE TABLE IF NOT EXISTS %s.%s (key text, column1 int, %s, PRIMARY KEY ((key), column1)) WITH compression = {'sstable_compression': ''}";

        String values = IntStream.range(0, colsPerRow).mapToObj(i -> "value"+i+" text").collect(Collectors.joining(", "));
        return String.format(createTblQuery, keyspaceName, tableName, values);
    }
}
