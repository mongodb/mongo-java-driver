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

import org.bson.BsonDocument;
import org.bson.UuidRepresentation;
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
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import org.jasonjson.core.JsonArray;
import org.jasonjson.core.JsonObject;
import org.jasonjson.core.JsonParser;

import static org.bson.internal.CodecRegistryHelper.createRegistry;

/*
 * An implementation of FindIterable for the HTTP driver.
 */
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

    FindIterator(final String collectionName, final String dbname, final @Nullable Bson filter, final String hostURL) {

        BsonDocument filterDoc = filter.toBsonDocument(BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation));
        String jsonFilter = filterDoc.toJson();
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
    public FindIterable<TResult> filter(final Bson filter) {
        BsonDocument filterDoc = filter.toBsonDocument(BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation));
        this.filter = filterDoc.toJson();
        return this;
    }

    @Override
    public FindIterable<TResult> limit(final int limit) {
       this.limit = limit;
       return this;
    }

    @Override
    public FindIterable<TResult> skip(final int skip) {
        this.skip = skip;
        return this;
    }

    @Override
    public FindIterable<TResult> maxTime(final long maxTime, final TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindIterable<TResult> maxAwaitTime(final long maxAwaitTime, final TimeUnit timeUnit) {
        return this;
    }

    @Override
    public FindIterable<TResult> modifiers(final Bson modifiers) {
        return this;
    }

    @Override
    public FindIterable<TResult> projection(final Bson projection) {
        this.projection = projection.toBsonDocument(BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation)).toJson();
        return this;
    }

    @Override
    public FindIterable<TResult> sort(final Bson sort) {
        String sortStr = sort.toBsonDocument(BsonDocument.class, createRegistry(codecRegistry, uuidRepresentation)).toJson();
        this.sortField = sortStr;
        return this;
    }

    @Override
    public FindIterable<TResult> noCursorTimeout(final boolean noCursorTimeout) {
        return this;
    }

    @Override
    public FindIterable<TResult> oplogReplay(final boolean oplogReplay) {
        return this;
    }

    @Override
    public FindIterable<TResult> partial(final boolean partial) {
        return this;
    }

    @Override
    public FindIterable<TResult> cursorType(final CursorType cursorType) {
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
        try {
            result = (TResult) Document.parse(getResponse(this.filter,  "ASC", ""));
        }
        catch (Exception e){
            result = (TResult) new Document();
        }
        return result;
    }

    @Override
    public <U> MongoIterable<U> map(final Function<TResult, U> mapper) {
        return null;
    }

    @Override
    public void forEach(final Block<? super TResult> block) {

    }

    @Override
    public <A extends Collection<? super TResult>> A into(final A target) {
        String res = getResponse(this.filter, "ASC", "");
        JsonParser parser = new JsonParser();
        JsonArray jsonArray = (JsonArray) parser.parse(res);
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonObject jsonObject1 = jsonArray.get(i).getAsJsonObject();
            Document doc = Document.parse(jsonObject1.toString());
            target.add((TResult) doc);
        }
        return target;
    }

    @Override
    public FindIterable<TResult> batchSize(final int batchSize) {
        return null;
    }

    @Override
    public FindIterable<TResult> collation(final Collation collation) {
        return null;
    }

    @Override
    public FindIterable<TResult> comment(final String comment) {
        return null;
    }

    @Override
    public FindIterable<TResult> hint(final Bson hint) {
        return null;
    }

    @Override
    public FindIterable<TResult> hintString(final String hint) {
        return null;
    }

    @Override
    public FindIterable<TResult> max(final Bson max) {
        return null;
    }

    @Override
    public FindIterable<TResult> min(final Bson min) {
        return null;
    }

    @Override
    public FindIterable<TResult> maxScan(final long maxScan) {
        return null;
    }

    @Override
    public FindIterable<TResult> returnKey(final boolean returnKey) {
        return null;
    }

    @Override
    public FindIterable<TResult> showRecordId(final boolean showRecordId) {
        return null;
    }
    /*
     *  snapshot
     */
    @Override
    public FindIterable<TResult> snapshot(final boolean snapshot) {
        return null;
    }

    public static String getResult(final String mainQuery, final String hostURL) throws IOException {
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


    private Query getQuery(final String filter, final String sortDirection) {
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

    private String getResponse(final String filter, final String sortField, final String sortDirection){
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
