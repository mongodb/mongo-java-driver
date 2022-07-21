/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.http;

import java.util.HashMap;
import java.util.Map;

public class Query {
    private Map<String, String> queryKeyValues;

    Query(final Map<String, String> queryKeyValues) {
        this.queryKeyValues = queryKeyValues;
    }

    Query() {
        this.queryKeyValues = new HashMap<String, String>();
    }

    Query(final String query) {
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

    public void setQuery(final String query) {
        String[] keyValues = query.split("&");
        for (String keyValue : keyValues) {
            String[] keyValuePair = keyValue.split("=");
            this.queryKeyValues.put(keyValuePair[0], keyValuePair[1]);
        }
    }

    public void appendQueryKeyValue(final String key, final String value) {
        this.queryKeyValues.put(key, value);
    }

    public void removeQueryKeyValue(final String key) {
        this.queryKeyValues.remove(key);
    }

    public String getQueryKeyValue(final String key) {
        return this.queryKeyValues.get(key);
    }

    public Map<String, String> getQueryKeyValues() {
        return this.queryKeyValues;
    }

    public void setQueryKeyValues(final Map<String, String> queryKeyValues) {
        this.queryKeyValues = queryKeyValues;
    }

    public String toString() {
        return getQuery();
    }

}
