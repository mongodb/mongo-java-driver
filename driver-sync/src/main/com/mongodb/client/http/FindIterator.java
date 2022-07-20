package com.mongodb.client.http;

import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import org.bson.Document;
import org.bson.conversions.Bson;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.jasonjson.core.JsonArray;
import org.jasonjson.core.JsonObject;
import org.jasonjson.core.JsonParser;

public class FindIterator<TDocument, TResult> implements FindIterable<TResult> {


    private final String dbname;
    private final String collectionName;
    private final int maxLimit;
    private int limit;
    private  String filter;
    private final String hostURL;
    FindIterator(String collectionName, String dbname, @Nullable Bson filter, String hostURL) {
        this.collectionName = collectionName;

        if (filter != null) {
            this.filter = filter.toString();
        } else {
            this.filter = "Filter{}";
        }
        this.dbname = dbname;
        this.hostURL = hostURL;
        this.limit = 1000;
        this.maxLimit = 1000;
    }

    @Override
    public FindIterable<TResult> filter(Bson filter) {
        this.filter = filter.toString();
        return this;
    }

    @Override
    public FindIterable<TResult> limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public FindIterable<TResult> skip(int skip) {
        return null;
    }

    @Override
    public FindIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public FindIterable<TResult> modifiers(Bson modifiers) {
        return null;
    }

    @Override
    public FindIterable<TResult> projection(Bson projection) {
        return null;
    }

    @Override
    public FindIterable<TResult> sort(Bson sort) {
        return null;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(boolean noCursorTimeout) {
        return null;
    }

    @Override
    public FindIterable<TResult> oplogReplay(boolean oplogReplay) {
        return null;
    }

    @Override
    public FindIterable<TResult> partial(boolean partial) {
        return null;
    }

    @Override
    public FindIterable<TResult> cursorType(CursorType cursorType) {
        return null;
    }

    @Override
    public MongoCursor<TResult> iterator() {
        return null;
    }

    @Override
    public MongoCursor<TResult> cursor() {
        return null;
    }

    @Override
    public TResult first() {
        Document doc = Document.parse(getResponse(this.filter, 1, "ASC", ""));
        return (TResult) doc;
    }

    @Override
    public <U> MongoIterable<U> map(Function<TResult, U> mapper) {
        return null;
    }

    @Override
    public void forEach(Block<? super TResult> block) {

    }

    @Override
    public <A extends Collection<? super TResult>> A into(A target) {
        String res = getResponse(this.filter, this.limit, "ASC", "");
        JsonParser parser = new JsonParser();
        JsonArray jsonArray = (JsonArray) parser.parse(res);;
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonObject1 = jsonArray.get(i).getAsJsonObject();
            Document doc = Document.parse(jsonObject1.toString());
            target.add((TResult) doc);
        }
        return target;
    }

    @Override
    public FindIterable<TResult> batchSize(int batchSize) {
        return null;
    }

    @Override
    public FindIterable<TResult> collation(Collation collation) {
        return null;
    }

    @Override
    public FindIterable<TResult> comment(String comment) {
        return null;
    }

    @Override
    public FindIterable<TResult> hint(Bson hint) {
        return null;
    }

    @Override
    public FindIterable<TResult> hintString(String hint) {
        return null;
    }

    @Override
    public FindIterable<TResult> max(Bson max) {
        return null;
    }

    @Override
    public FindIterable<TResult> min(Bson min) {
        return null;
    }

    @Override
    public FindIterable<TResult> maxScan(long maxScan) {
        return null;
    }

    @Override
    public FindIterable<TResult> returnKey(boolean returnKey) {
        return null;
    }

    @Override
    public FindIterable<TResult> showRecordId(boolean showRecordId) {
        return null;
    }

    @Override
    public FindIterable<TResult> snapshot(boolean snapshot) {
        return null;
    }

    public static String getResult(String mainQuery, String hostURL) throws IOException {
        URL url = new URL(hostURL);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setRequestMethod("POST");

        httpConn.setRequestProperty("x-requested-with", "XMLHttpRequest");
        httpConn.setRequestProperty("content-type", "application/x-www-form-urlencoded");

        httpConn.setDoOutput(true);
        OutputStreamWriter writer = new OutputStreamWriter(httpConn.getOutputStream());
        writer.write(mainQuery);
        writer.flush();
        writer.close();
        httpConn.getOutputStream().close();

        InputStream responseStream = httpConn.getResponseCode() / 100 == 2
                ? httpConn.getInputStream()
                : httpConn.getErrorStream();
        Scanner s = new Scanner(responseStream).useDelimiter("\\A");
        String response = s.hasNext() ? s.next() : "";
        return response;
    }

    private String queryToJson(String query) {
        if (query.startsWith("And Filter")) {
            query = query.substring(11);
            query = queryToJson(query);
            query = query.substring(1, query.length() - 1);
            query = mapToJson(getMap(query));
        } else if (query.startsWith("filters=")) {
            query = query.substring(8);
            return query;
        }
        else if (query.startsWith("Filter")) {
            return mapToJson(getMap(query));
        }
        else {
            return "";
        }
        return query;
    }

    private Map<String, String> getMap(String query) {
        Map<String, String> map = new HashMap<String, String>();
        String[] fields = query.split("Filter\\{");
        for (String field : fields) {
            if (field.length() > 0) {
                String[] fieldParts = field.split("\\}");
                String fieldAct = fieldParts[0];
                String[] keyValue = fieldAct.split(", ");
                String key = keyValue[0].split("=")[1];
                key = key.substring(1, key.length() - 1);
                String value = keyValue[1].split("=")[1];
                map.put(key.toString(), value.toString());
            }
        }
        return map;
    }
    private String mapToJson(Map<String, String> map) {
        String json = "{";
        for (Map.Entry<String, String> entry : map.entrySet()) {
            json += "\"" + entry.getKey() + "\":\"" + entry.getValue() + "\",";
        }
        json = json.substring(0, json.length() - 1);
        json += "}";
        return json;
    }

    private Query getQuery(String filter, int limit, String sortField, String sortDirection) {
        Query query = new Query();
        query.appendQueryKeyValue("Service", "MONGO");
        query.appendQueryKeyValue("partnerId", "0");
        query.appendQueryKeyValue("serverType", "GLOBAL");
        query.appendQueryKeyValue("collectionName", collectionName);
        query.appendQueryKeyValue("queryType", "general");
        query.appendQueryKeyValue("limit", String.valueOf(limit));
        query.appendQueryKeyValue("sortDirection", sortDirection);
        query.appendQueryKeyValue("sortField", sortField);
        query.appendQueryKeyValue("includeFields", "");
        query.appendQueryKeyValue("query", queryToJson(filter));
        return query;
    }

    private String getResponse(String filter, int limit, String sortField, String sortDirection){
        Query query = getQuery(filter, limit, sortDirection, sortField);
        String queryString = query.toString();
        String response = "";
        try {
            response = getResult(queryString, hostURL);
        } catch (IOException e) {
            return "";
        }
        return response;
    }

}
