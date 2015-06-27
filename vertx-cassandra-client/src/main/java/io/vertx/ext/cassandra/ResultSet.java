package io.vertx.ext.cassandra;

import java.util.List;

public class ResultSet {

    private List<String> names;
    private List<List<Object>> values;
    private ExecutionInfo metaInformation;

    public ResultSet(List<String> names, List<List<Object>> values, ExecutionInfo metaInformation) {
        this.names = names;
        this.values = values;
        this.metaInformation = metaInformation;
    }

    public int size() {
        return values.size();
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public ExecutionInfo getExecutionInfo() {
        return metaInformation;
    }

}
