package com.mongodb.client.http;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.FindOptions;
import org.bson.BsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistries;
import com.mongodb.Block;
import com.mongodb.CursorType;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Collation;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
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

import static org.bson.internal.CodecRegistryHelper.createRegistry;

public class FindIterator<TDocument, TResult> implements FindIterable<TResult> {


    private final String dbname;
    private final String collectionName;
    private  String filter;
    private final String hostURL;

    private int limit;

    private String  projection;

    private String sortField;

    private int skip;


    private CodecRegistry codecRegistry = com.mongodb.MongoClientSettings.getDefaultCodecRegistry();

    private UuidRepresentation uuidRepresentation = UuidRepresentation.JAVA_LEGACY;

    FindIterator(String collectionName, String dbname, @Nullable Bson filter, String hostURL) {

        BsonDocument FilterDoc = filter.toBsonDocument( BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation));
        String jsonFilter = FilterDoc.toJson();
        this.collectionName = collectionName;
        if (filter != null) {
            this.filter = jsonFilter;
        } else {
            this.filter = "{}";
        }
        this.dbname = dbname;
        this.hostURL = hostURL;
        this.limit = 1000;
        this.projection = null;
        this.sortField = null;
        this.skip = 0;

    }

    @Override
    public FindIterable<TResult> filter(Bson filter) {
        BsonDocument filterDoc = filter.toBsonDocument( BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation));
        this.filter = filterDoc.toJson();
        return this;
    }

    @Override
    public FindIterable<TResult> limit(int limit) {
       this.limit = limit;
       return this;
    }

    @Override
    public FindIterable<TResult> skip(int skip) {
        this.skip = skip;
        return this;
    }

    @Override
    public FindIterable<TResult> maxTime(long maxTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindIterable<TResult> modifiers(Bson modifiers) {
        return this;
    }

    @Override
    public FindIterable<TResult> projection(Bson projection) {
        this.projection = projection.toBsonDocument( BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation)).toJson();
        return this;
    }

    @Override
    public FindIterable<TResult> sort(Bson sort) {
        String sortStr = sort.toBsonDocument( BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation)).toJson();
        this.sortField = sortStr;
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(boolean noCursorTimeout) {
        return this;
    }

    @Override
    public FindIterable<TResult> oplogReplay(boolean oplogReplay) {
        return this;
    }

    @Override
    public FindIterable<TResult> partial(boolean partial) {
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(CursorType cursorType) {
        return this;
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
        this.limit = 1;
        TResult result;
        try{
            System.out.println("first");
            result =(TResult) Document.parse(getResponse(this.filter,  "ASC", ""));
        }
        catch (Exception e){
            result = (TResult) new Document();
        }
        return result;
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
        String res = getResponse(this.filter, "ASC", "");
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


    private Query getQuery(String filter, String sortDirection) {
        Query query = new Query();
        query.appendQueryKeyValue("Service", "MONGO");
        query.appendQueryKeyValue("partnerId", "0");
        query.appendQueryKeyValue("serverType", "GLOBAL");
        query.appendQueryKeyValue("collectionName", collectionName);
        query.appendQueryKeyValue("queryType", "general");
        query.appendQueryKeyValue("limit", String.valueOf(this.limit));
        query.appendQueryKeyValue("sortField", this.sortField);
        query.appendQueryKeyValue("includeFields", this.projection);
        query.appendQueryKeyValue("query", filter);
        return query;
    }

    private String getResponse(String filter ,String sortField, String sortDirection){
        Query query = getQuery(filter, sortField);
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
