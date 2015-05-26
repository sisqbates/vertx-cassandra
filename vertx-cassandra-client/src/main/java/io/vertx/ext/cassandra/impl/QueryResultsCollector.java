package io.vertx.ext.cassandra.impl;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.cassandra.ResultSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Row;

public class QueryResultsCollector {

    public ResultSet collectResults(com.datastax.driver.core.ResultSet cassandraResultSet) {

        List<String> names = cassandraResultSet.getColumnDefinitions().asList().stream().map(d -> d.getName())
                .collect(Collectors.toList());
        List<JsonArray> values = cassandraResultSet.all().stream().map(r -> this.rowToArray(r, names.size()))
                .collect(Collectors.toList());

        return new ResultSet(names, values);
    }

    private JsonArray rowToArray(Row r, int size) {
        JsonArray result = new JsonArray();

        for (int i = 0; i < size; i++) {
            Object o = r.getObject(i);
            if (o == null) {
                result.addNull();
            } else if (o instanceof Map) {
                JsonObject inner = new JsonObject();
                ((Map<?, ?>) o).entrySet().stream().forEach(e -> inner.put(e.getKey().toString(), e.getValue()));
                result.add(inner);
            } else if (o instanceof List || o instanceof Set) {
                JsonArray inner = new JsonArray();
                ((Collection<?>) o).stream().forEach(e -> inner.add(e));
                result.add(inner);
            } else {
                result.add(o);
            }
        }

        return result;
    }
}
