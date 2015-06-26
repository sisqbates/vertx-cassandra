package io.vertx.ext.cassandra.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.datastax.driver.core.Row;

import io.vertx.ext.cassandra.ExecutionInfo;
import io.vertx.ext.cassandra.ResultSet;

public class QueryResultsCollector {

    public ResultSet collectResults(com.datastax.driver.core.ResultSet cassandraResultSet) {

        List<String> names = this.getColumnNames(cassandraResultSet);

        List<List<Object>> values = new ArrayList<>();
        Iterator<Row> it = cassandraResultSet.iterator();
        while (cassandraResultSet.getAvailableWithoutFetching() > 0) {
            values.add(this.rowToArray(it.next(), names.size()));
        }

        ExecutionInfo metaInformation = this.processExecutionInfo(cassandraResultSet);

        return new ResultSet(names, values, metaInformation);
    }

    private List<String> getColumnNames(com.datastax.driver.core.ResultSet cassandraResultSet) {
        return cassandraResultSet.getColumnDefinitions().asList().stream().map(d -> d.getName())
                .collect(Collectors.toList());
    }

    private List<Object> rowToArray(Row r, int size) {
        List<Object> result = new ArrayList<>(size);

        for (int i = 0; i < size; i++)
            result.add(r.getObject(i));

        return result;
    }

    private ExecutionInfo processExecutionInfo(com.datastax.driver.core.ResultSet cassandraResultSet) {
        ExecutionInfo result = new ExecutionInfo();

        // TODO: From java driver docs: "Please note that the writing of the
        // trace is done asynchronously in Cassandra. So accessing the trace too
        // soon after the query may result in the trace being incomplete."
        //
        // So maybe we should publish another method in the client to get
        // information from a trace

        result.setQueryTrace(Objects.toString(cassandraResultSet.getExecutionInfo().getQueryTrace(), null));
        return result;
    }

}
