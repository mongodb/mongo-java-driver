/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.codecs.BsonDocumentCodec;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocol;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNull;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;

/**
 * An operation that executes a count.
 *
 * @since 3.0
 */
public class CountOperation implements AsyncReadOperation<Long>, ReadOperation<Long> {
    private final MongoNamespace namespace;
    private BsonDocument criteria;
    private BsonValue hint;
    private long skip;
    private long limit;
    private long maxTimeMS;

    /**
     * This is going away soon.
     *
     * @param namespace the namespace to execute the count operation on
     * @param find find
     */
    public CountOperation(final MongoNamespace namespace, final Find find) {
        this.namespace = notNull("namespace", namespace);
        criteria = find.getFilter();
        hint = find.getHint();
        skip = find.getSkip();
        limit = find.getLimit();
        maxTimeMS = find.getOptions().getMaxTimeMS();
    }

    /**
     * Construct a new instance.
     *
     * @param namespace the namespace to execute the count operation on
     */
    public CountOperation(final MongoNamespace namespace) {
        this.namespace = notNull("namespace", namespace);
    }

    /**
     * Gets the query criteria.
     *
     * @return the query criteria
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public BsonDocument getCriteria() {
        return criteria;
    }

    /**
     * Sets the criteria to apply to the query.
     *
     * @param criteria the criteria, which may be null.
     * @mongodb.driver.manual manual/reference/method/db.collection.find/ Criteria
     */
    public void setCriteria(final BsonDocument criteria) {
        this.criteria = criteria;
    }

    /**
     * Gets the hint to apply.
     *
     * @return the hint, which should describe an existing
     */
    public BsonValue getHint() {
        return hint;
    }

    /**
     * Sets the hint to apply.
     *
     * @param hint a value describing the index which should be used for this operation.
     */
    public void setHint(final BsonValue hint) {
        this.hint = hint;
    }

    /**
     * Gets the limit to apply.  The default is 0, which means there is no limit.
     *
     * @return the limit
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Sets the limit to apply.
     *
     * @param limit the limit
     * @mongodb.driver.manual manual/reference/method/cursor.limit/#cursor.limit Limit
     */
    public void setLimit(final long limit) {
        this.limit = limit;
    }

    /**
     * Gets the number of documents to skip.  The default is 0.
     *
     * @return the number of documents to skip, which may be null
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public long getSkip() {
        return skip;
    }

    /**
     * Sets the number of documents to skip.
     *
     * @param skip the number of documents to skip
     * @mongodb.driver.manual manual/reference/method/cursor.skip/#cursor.skip Skip
     */
    public void setSkip(final long skip) {
        this.skip = skip;
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     */
    public void setMaxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
    }

    @Override
    public Long execute(final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), asCommandDocument(), new BsonDocumentCodec(),
                                             binding, transformer());
    }

    @Override
    public MongoFuture<Long> executeAsync(final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), asCommandDocument(),
                                                  new BsonDocumentCodec(), binding, transformer());
    }

    private Function<BsonDocument, Long> transformer() {
        return new Function<BsonDocument, Long>() {
            @Override
            public Long apply(final BsonDocument result) {
                return (result.getNumber("n")).longValue();
            }
        };
    }

    private BsonDocument asCommandDocument() {
        BsonDocument document = new BsonDocument("count", new BsonString(namespace.getCollectionName()));
        putIfNotNull(document, "query", criteria);
        putIfNotZero(document, "limit", limit);
        putIfNotZero(document, "skip", skip);
        putIfNotNull(document, "hint", hint);
        putIfNotZero(document, "maxTimeMS", maxTimeMS);
        return document;
    }
}
