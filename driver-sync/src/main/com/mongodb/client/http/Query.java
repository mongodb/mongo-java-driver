package com.mongodb.client.http;

import java.util.HashMap;
import java.util.Map;

public class Query {
    private Map<String, String> queryKeyValues;

    Query(Map<String, String> queryKeyValues) {
        this.queryKeyValues = queryKeyValues;
    }

    Query() {
        this.queryKeyValues = new HashMap<String, String>();
    }

    Query (String query) {
        this.queryKeyValues = new HashMap<String, String>();
        String[] keyValues = query.split("&");
        for (String keyValue : keyValues) {
            String[] keyValuePair = keyValue.split("=");
            this.queryKeyValues.put(keyValuePair[0], keyValuePair[1]);
        }
    }

    public String getQuery() {
        String query = "";
        for (String key : queryKeyValues.keySet()) {
            query += key + "=" + queryKeyValues.get(key) + "&";
        }
        return query.substring(0, query.length() - 1);
    }

    public void setQuery(String query) {
        String[] keyValues = query.split("&");
        for (String keyValue : keyValues) {
            String[] keyValuePair = keyValue.split("=");
            this.queryKeyValues.put(keyValuePair[0], keyValuePair[1]);
        }
    }

    public void appendQueryKeyValue(String key, String value) {
        this.queryKeyValues.put(key, value);
    }

    public void removeQueryKeyValue(String key) {
        this.queryKeyValues.remove(key);
    }

    public String getQueryKeyValue(String key) {
        return this.queryKeyValues.get(key);
    }

    public Map<String, String> getQueryKeyValues() {
        return this.queryKeyValues;
    }

    public void setQueryKeyValues(Map<String, String> queryKeyValues) {
        this.queryKeyValues = queryKeyValues;
    }

    public String toString() {
        return getQuery();
    }

}
