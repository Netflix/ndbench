package com.netflix.ndbench.plugin.cockroachdb.operations;

import java.sql.Connection;
import java.sql.SQLException;

public interface CockroachDBRetryableTransaction {
    void run(Connection conn)
            throws SQLException;
}
