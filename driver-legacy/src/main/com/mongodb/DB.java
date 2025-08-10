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

package com.mongodb;

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.client.internal.MongoIterableImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.DBCreateViewOptions;
import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.operation.CommandReadOperation;
import com.mongodb.internal.operation.CreateCollectionOperation;
import com.mongodb.internal.operation.CreateViewOperation;
import com.mongodb.internal.operation.DropDatabaseOperation;
import com.mongodb.internal.operation.ListCollectionsOperation;
import com.mongodb.internal.operation.ReadOperationCursor;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.DBCollection.createWriteConcernException;
import static com.mongodb.MongoNamespace.checkDatabaseNameValidity;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * A thread-safe client view of a logical database in a MongoDB cluster. A DB instance can be achieved from a {@link MongoClient} instance
 * using code like:
 * <pre>
 * {@code
 * MongoClient mongoClient = new MongoClient();
 * DB db = mongoClient.getDB("<db name>");
 * }
 * </pre>
 *
 * See {@link MongoClient#getDB(String)} for further information about the effective deprecation of this class.
 *
 * @mongodb.driver.manual reference/glossary/#term-database Database
 * @see MongoClient
 */
@ThreadSafe
@SuppressWarnings("deprecation")
public class DB {
    private final MongoClient mongo;
    private final String name;
    private final OperationExecutor executor;
    private final ConcurrentHashMap<String, DBCollection> collectionCache;
    private final Codec<DBObject> commandCodec;
    private volatile ReadPreference readPreference;
    private volatile WriteConcern writeConcern;
    private volatile ReadConcern readConcern;

    DB(final MongoClient mongo, final String name, final OperationExecutor executor) {
        checkDatabaseNameValidity(name);
        this.mongo = mongo;
        this.name = name;
        this.executor = executor;
        this.collectionCache = new ConcurrentHashMap<>();
        this.commandCodec = new DBObjectCodec(mongo.getCodecRegistry());
    }

    /**
     * Gets the MongoClient instance
     *
     * @return the MongoClient instance that this database was constructed from
     * @throws IllegalStateException if this DB was not created from a MongoClient instance
     * @since 3.9
     */
    public MongoClient getMongoClient() {
        return mongo;
    }

    /**
     * Sets the read preference for this database. Will be used as default for read operations from any collection in this database. See the
     * documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference {@code ReadPreference} to use
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    /**
     * Sets the write concern for this database. It will be used for write operations to any collection in this database. See the
     * documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern {@code WriteConcern} to use
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the read preference for this database.
     *
     * @return {@code ReadPreference} to be used for read operations, if not specified explicitly
     * @mongodb.driver.manual core/read-preference/ Read Preference
     */
    public ReadPreference getReadPreference() {
        return readPreference != null ? readPreference : mongo.getReadPreference();
    }

    /**
     * Gets the write concern for this database.
     *
     * @return {@code WriteConcern} to be used for write operations, if not specified explicitly
     * @mongodb.driver.manual core/write-concern/ Write Concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern != null ? writeConcern : mongo.getWriteConcern();
    }

    /**
     * Sets the read concern for this database.
     *
     * @param readConcern the read concern to use for this collection
     * @since 3.3
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public void setReadConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
    }

    /**
     * Get the read concern for this database.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since 3.3
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern != null ? readConcern : mongo.getReadConcern();
    }

    /**
     * Gets a collection with a given name.
     *
     * @param name the name of the collection to return
     * @return the collection
     * @throws IllegalArgumentException if the name is invalid
     * @see MongoNamespace#checkCollectionNameValidity(String)
     */
    public DBCollection getCollection(final String name) {
        DBCollection collection = collectionCache.get(name);
        if (collection != null) {
            return collection;
        }

        collection = new DBCollection(name, this, executor);
        if (mongo.getMongoClientOptions().getDbDecoderFactory() != DefaultDBDecoder.FACTORY) {
            collection.setDBDecoderFactory(mongo.getMongoClientOptions().getDbDecoderFactory());
        }
        if (mongo.getMongoClientOptions().getDbEncoderFactory() != DefaultDBEncoder.FACTORY) {
            collection.setDBEncoderFactory(mongo.getMongoClientOptions().getDbEncoderFactory());
        }
        DBCollection old = collectionCache.putIfAbsent(name, collection);
        return old != null ? old : collection;
    }

    /**
     * Drops this database. Removes all data on disk. Use with caution.
     *
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/command/dropDatabase/ Drop Database
     */
    public void dropDatabase() {
        try {
            getExecutor().execute(new DropDatabaseOperation(getName(), getWriteConcern()), getReadConcern());
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }

    /**
     * Returns the name of this database.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns a set containing the names of all collections in this database.
     *
     * @return the names of collections in this database
     * @throws MongoException if the operation failed
     * @mongodb.driver.manual reference/method/db.getCollectionNames/ getCollectionNames()
     */
    public Set<String> getCollectionNames() {
        List<String> collectionNames =
                new MongoIterableImpl<DBObject>(null, executor, ReadConcern.DEFAULT, primary(),
                                                mongo.getMongoClientOptions().getRetryReads(), DB.this.getTimeoutSettings()) {
                    @Override
                    public ReadOperationCursor<DBObject> asReadOperation() {
                        return new ListCollectionsOperation<>(name, commandCodec).nameOnly(true);
                    }

                    @Override
                    protected OperationExecutor getExecutor() {
                        return executor;
                    }
                }.map(result -> (String) result.get("name")).into(new ArrayList<>());
        Collections.sort(collectionNames);
        return new LinkedHashSet<>(collectionNames);
    }

    /**
     * <p>Creates a collection with a given name and options. If the collection already exists,
     * this throws a {@code CommandFailureException}.</p>
     *
     * <p>Possible options:</p>
     * <ul>
     *     <li> <b>capped</b> ({@code boolean}) - Enables a collection cap. False by default. If enabled,
     *     you must specify a size parameter. </li>
     *     <li> <b>size</b> ({@code int}) - If capped is true, size specifies a maximum size in bytes for the capped collection. When
     *     capped is false, you may use size to preallocate space. </li>
     *     <li> <b>max</b> ({@code int}) -   Optional. Specifies a maximum "cap" in number of documents for capped collections. You must
     *     also specify size when specifying max. </li>
     * </ul>
     * <p>Note that if the {@code options} parameter is {@code null}, the creation will be deferred to when the collection is written
     * to.</p>
     *
     * @param collectionName the name of the collection to return
     * @param options        options
     * @return the collection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException for all other failures
     * @mongodb.driver.manual reference/method/db.createCollection/ createCollection()
     */
    public DBCollection createCollection(final String collectionName, @Nullable final DBObject options) {
        if (options != null) {
            try {
                executor.execute(getCreateCollectionOperation(collectionName, options), getReadConcern());
            } catch (MongoWriteConcernException e) {
                throw createWriteConcernException(e);
            }
        }
        return getCollection(collectionName);
    }

    /**
     * Creates a view with the given name, backing collection/view name, and aggregation pipeline that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @return the view as a DBCollection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException for all other failures
     * @since 3.4
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    public DBCollection createView(final String viewName, final String viewOn, final List<? extends DBObject> pipeline) {
        return createView(viewName, viewOn, pipeline, new DBCreateViewOptions());
    }

    /**
     * Creates a view with the given name, backing collection/view name, aggregation pipeline, and options that defines the view.
     *
     * @param viewName the name of the view to create
     * @param viewOn   the backing collection/view for the view
     * @param pipeline the pipeline that defines the view
     * @param options  the options for creating the view
     * @return the view as a DBCollection
     * @throws MongoCommandException if the server is unable to create the collection
     * @throws WriteConcernException if the {@code WriteConcern} specified on this {@code DB} could not be satisfied
     * @throws MongoException for all other failures
     * @since 3.4
     * @mongodb.server.release 3.4
     * @mongodb.driver.manual reference/command/create Create Command
     */
    public DBCollection createView(final String viewName, final String viewOn, final List<? extends DBObject> pipeline,
                                   final DBCreateViewOptions options) {
        try {
            notNull("options", options);
            DBCollection view = getCollection(viewName);
            executor.execute(new CreateViewOperation(name, viewName, viewOn,
                    view.preparePipeline(pipeline), writeConcern)
                    .collation(options.getCollation()), getReadConcern());
            return view;
        } catch (MongoWriteConcernException e) {
            throw createWriteConcernException(e);
        }
    }


    private CreateCollectionOperation getCreateCollectionOperation(final String collectionName, final DBObject options) {
        if (options.get("size") != null && !(options.get("size") instanceof Number)) {
            throw new IllegalArgumentException("'size' should be Number");
        }
        if (options.get("max") != null && !(options.get("max") instanceof Number)) {
            throw new IllegalArgumentException("'max' should be Number");
        }
        if (options.get("capped") != null && !(options.get("capped") instanceof Boolean)) {
            throw new IllegalArgumentException("'capped' should be Boolean");
        }
        if (options.get("autoIndexId") != null && !(options.get("autoIndexId") instanceof Boolean)) {
            throw new IllegalArgumentException("'autoIndexId' should be Boolean");
        }
        if (options.get("storageEngine") != null && !(options.get("storageEngine") instanceof DBObject)) {
            throw new IllegalArgumentException("'storageEngine' should be DBObject");
        }
        if (options.get("indexOptionDefaults") != null && !(options.get("indexOptionDefaults") instanceof DBObject)) {
            throw new IllegalArgumentException("'indexOptionDefaults' should be DBObject");
        }
        if (options.get("validator") != null && !(options.get("validator") instanceof DBObject)) {
            throw new IllegalArgumentException("'validator' should be DBObject");
        }
        if (options.get("validationLevel") != null && !(options.get("validationLevel") instanceof String)) {
            throw new IllegalArgumentException("'validationLevel' should be String");
        }
        if (options.get("validationAction") != null && !(options.get("validationAction") instanceof String)) {
            throw new IllegalArgumentException("'validationAction' should be String");
        }

        boolean capped = false;
        boolean autoIndex = true;
        long sizeInBytes = 0;
        long maxDocuments = 0;
        BsonDocument storageEngineOptions = null;
        BsonDocument indexOptionDefaults = null;
        BsonDocument validator = null;
        ValidationLevel validationLevel = null;
        ValidationAction validationAction = null;

        if (options.get("capped") != null) {
            capped = (Boolean) options.get("capped");
        }
        if (options.get("size") != null) {
            sizeInBytes = ((Number) options.get("size")).longValue();
        }
        if (options.get("autoIndexId") != null) {
            autoIndex = (Boolean) options.get("autoIndexId");
        }
        if (options.get("max") != null) {
            maxDocuments = ((Number) options.get("max")).longValue();
        }
        if (options.get("storageEngine") != null) {
            storageEngineOptions = wrap((DBObject) options.get("storageEngine"));
        }
        if (options.get("indexOptionDefaults") != null) {
            indexOptionDefaults = wrap((DBObject) options.get("indexOptionDefaults"));
        }
        if (options.get("validator") != null) {
            validator = wrap((DBObject) options.get("validator"));
        }
        if (options.get("validationLevel") != null) {
            validationLevel = ValidationLevel.fromString((String) options.get("validationLevel"));
        }
        if (options.get("validationAction") != null) {
            validationAction = ValidationAction.fromString((String) options.get("validationAction"));
        }
        Collation collation = DBObjectCollationHelper.createCollationFromOptions(options);
        return new CreateCollectionOperation(getName(), collectionName,
                getWriteConcern())
                   .capped(capped)
                   .collation(collation)
                   .sizeInBytes(sizeInBytes)
                   .autoIndex(autoIndex)
                   .maxDocuments(maxDocuments)
                   .storageEngineOptions(storageEngineOptions)
                   .indexOptionDefaults(indexOptionDefaults)
                   .validator(validator)
                   .validationLevel(validationLevel)
                   .validationAction(validationAction);
    }

    /**
     * Executes a database command. This method constructs a simple DBObject using {@code command} as the field name and {@code true} as its
     * value, and calls {@link DB#command(DBObject, ReadPreference) } with the default read preference for the database.
     *
     * @param command command to execute
     * @return result of command from the database
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final String command) {
        return command(new BasicDBObject(command, Boolean.TRUE), getReadPreference());
    }

    /**
     * Executes a database command. This method calls {@link DB#command(DBObject, ReadPreference) } with the default read preference for the
     * database.
     *
     * @param command {@code DBObject} representation of the command to be executed
     * @return result of the command execution
     * @throws MongoException  if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject command) {
        return command(command, getReadPreference());
    }

    /**
     * Executes a database command. This method calls {@link DB#command(DBObject, ReadPreference, DBEncoder) } with the default read
     * preference for the database.
     *
     * @param command {@code DBObject} representation of the command to be executed
     * @param encoder {@link DBEncoder} to be used for command encoding
     * @return result of the command execution
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     */
    public CommandResult command(final DBObject command, final DBEncoder encoder) {
        return command(command, getReadPreference(), encoder);
    }

    /**
     * Executes a database command with the selected readPreference, and encodes the command using the given encoder.
     *
     * @param command        The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @param encoder        The DBEncoder that knows how to serialise the command
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject command, final ReadPreference readPreference, @Nullable final DBEncoder encoder) {
        try {
            return executeCommand(wrap(command, encoder), getCommandReadPreference(command, readPreference));
        } catch (MongoCommandException ex) {
            return new CommandResult(ex.getResponse(), getDefaultDBObjectCodec(), ex.getServerAddress());
        }
    }

    /**
     * Executes the command against the database with the given read preference.
     *
     * @param command        The {@code DBObject} representation the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of executing the command, success or failure
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final DBObject command, final ReadPreference readPreference) {
        return command(command, readPreference, null);
    }

    /**
     * Executes a database command. This method constructs a simple {@link DBObject} and calls {@link DB#command(DBObject, ReadPreference)
     * }.
     *
     * @param command        The name of the command to be executed
     * @param readPreference Where to execute the command - this will only be applied for a subset of commands
     * @return The result of the command execution
     * @throws MongoException if the command failed
     * @mongodb.driver.manual tutorial/use-database-commands Commands
     * @since 2.12
     */
    public CommandResult command(final String command, final ReadPreference readPreference) {
        return command(new BasicDBObject(command, true), readPreference);
    }

    /**
     * Gets another database on same server
     *
     * @param name name of the database
     * @return the DB for the given name
     */
    @SuppressWarnings("deprecation") // The old API (i.e. DB) will use deprecated methods.
    public DB getSisterDB(final String name) {
        return mongo.getDB(name);
    }

    /**
     * Checks to see if a collection with a given name exists on a server.
     *
     * @param collectionName a name of the collection to test for existence
     * @return {@code false} if no collection by that name exists, {@code true} if a match to an existing collection was found
     * @throws MongoException if the operation failed
     */
    public boolean collectionExists(final String collectionName) {
        Set<String> collectionNames = getCollectionNames();
        for (final String name : collectionNames) {
            if (name.equalsIgnoreCase(collectionName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "DB{name='" + name + '\'' + '}';
    }

    CommandResult executeCommand(final BsonDocument commandDocument, final ReadPreference readPreference) {
        return new CommandResult(executor.execute(
                new CommandReadOperation<>(getName(), commandDocument,
                        new BsonDocumentCodec()), readPreference, getReadConcern(), null), getDefaultDBObjectCodec());
    }

    OperationExecutor getExecutor() {
        return executor;
    }
    TimeoutSettings getTimeoutSettings() {
        return mongo.getTimeoutSettings();
    }

    private BsonDocument wrap(final DBObject document) {
        return new BsonDocumentWrapper<>(document, commandCodec);
    }

    private BsonDocument wrap(final DBObject document, @Nullable final DBEncoder encoder) {
        if (encoder == null) {
            return wrap(document);
        } else {
            return new BsonDocumentWrapper<>(document, new DBEncoderAdapter(encoder));
        }
    }

    /**
     * Determines the read preference that should be used for the given command.
     *
     * @param command             the {@link DBObject} representing the command
     * @param requestedPreference the preference requested by the client.
     * @return the read preference to use for the given command.  It will never return {@code null}.
     * @see com.mongodb.ReadPreference
     */
    ReadPreference getCommandReadPreference(final DBObject command, @Nullable final ReadPreference requestedPreference) {
        String comString = command.keySet().iterator().next().toLowerCase();
        boolean primaryRequired = !OBEDIENT_COMMANDS.contains(comString);

        if (primaryRequired) {
            return primary();
        } else if (requestedPreference == null) {
            return primary();
        } else {
            return requestedPreference;
        }
    }

    Codec<DBObject> getDefaultDBObjectCodec() {
        return new DBObjectCodec(getMongoClient().getCodecRegistry(),
                DBObjectCodec.getDefaultBsonTypeClassMap(),
                new DBCollectionObjectFactory())
                .withUuidRepresentation(getMongoClient().getMongoClientOptions().getUuidRepresentation());
    }

    @Nullable
    Long getTimeoutMS() {
        return mongo.getMongoClientOptions().getTimeout();
    }

    private static final Set<String> OBEDIENT_COMMANDS = new HashSet<>();

    static {
        OBEDIENT_COMMANDS.add("aggregate");
        OBEDIENT_COMMANDS.add("collstats");
        OBEDIENT_COMMANDS.add("count");
        OBEDIENT_COMMANDS.add("dbstats");
        OBEDIENT_COMMANDS.add("distinct");
        OBEDIENT_COMMANDS.add("geonear");
        OBEDIENT_COMMANDS.add("geosearch");
        OBEDIENT_COMMANDS.add("geowalk");
        OBEDIENT_COMMANDS.add("group");
        OBEDIENT_COMMANDS.add("listcollections");
        OBEDIENT_COMMANDS.add("listindexes");
        OBEDIENT_COMMANDS.add("parallelcollectionscan");
        OBEDIENT_COMMANDS.add("text");
    }
}
