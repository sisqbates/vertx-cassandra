package io.vertx.ext.cassandra;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class ResultSet {

    private List<String> names;
    private List<JsonArray> values;

    public ResultSet(List<String> names, List<JsonArray> values) {
        this.names = names;
        this.values = values;
    }

    public JsonObject toJson() {
        return new JsonObject().put("columns", names).put("values", values);
    }

}
