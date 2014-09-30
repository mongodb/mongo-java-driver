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

package com.mongodb;

import com.mongodb.util.JSON;
import org.bson.io.PoolOutputBuffer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.WriteCommandResultHelper.getBulkWriteException;
import static com.mongodb.WriteCommandResultHelper.getBulkWriteResult;
import static com.mongodb.WriteCommandResultHelper.hasError;
import static com.mongodb.QueryResultIterator.chooseBatchSize;
import static com.mongodb.WriteRequest.Type.INSERT;
import static com.mongodb.WriteRequest.Type.REMOVE;
import static com.mongodb.WriteRequest.Type.REPLACE;
import static com.mongodb.WriteRequest.Type.UPDATE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.bson.util.Assertions.isTrue;

@SuppressWarnings("deprecation")
class DBCollectionImpl extends DBCollection {

    // Add an extra 16K to the maximum allowed size of a query document, so that, for example,
    // a 16M document can be upserted via findAndModify
    private static final int QUERY_DOCUMENT_HEADROOM = 16 * 1024;

    private final DBApiLayer db;
    private final String namespace;

    DBCollectionImpl(final DBApiLayer db, String name){
        super(db, name );
        namespace = db._root + "." + name;
        this.db = db;
    }

    @Override
    QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                             ReadPreference readPref, DBDecoder decoder) {
        return find(ref, fields, numToSkip, batchSize, limit, options, readPref, decoder, DefaultDBEncoder.FACTORY.create());
    }

    @Override
    QueryResultIterator find(DBObject ref, DBObject fields, int numToSkip, int batchSize, int limit, int options,
                             ReadPreference readPref, DBDecoder decoder, DBEncoder encoder) {

        if (ref == null) {
            ref = new BasicDBObject();
        }

        if (willTrace()) {
            trace("find: " + namespace + " " + JSON.serialize(ref));
        }

        OutMessage query = OutMessage.query(this, options, numToSkip, chooseBatchSize(batchSize, limit, 0), ref, fields, readPref,
                                            encoder, db.getMongo().getMaxBsonObjectSize() + QUERY_DOCUMENT_HEADROOM);

        Response res = db.getConnector().call(_db, this, query, null, 2, readPref, decoder);

        return new QueryResultIterator(db, this, res, batchSize, limit, options, decoder);
    }

    public Cursor aggregate(final List<DBObject> pipeline, final AggregationOptions options,
                            final ReadPreference readPreference) {

        if(options == null) {
            throw new IllegalArgumentException("options can not be null");
        }
        DBObject last = pipeline.get(pipeline.size() - 1);

        DBObject command = prepareCommand(pipeline, options);

        final CommandResult res = _db.command(command, getOptions(), readPreference);
        res.throwOnError();

        String outCollection = (String) last.get("$out");
        if (outCollection != null) {
            DBCollection collection = _db.getCollection(outCollection);
            return new DBCursor(collection, new BasicDBObject(), null, ReadPreference.primary());
        } else {
            Integer batchSize = options.getBatchSize();
            return new QueryResultIterator(res, db, this, batchSize == null ? 0 : batchSize, getDecoder(), res.getServerUsed());
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Cursor> parallelScan(final ParallelScanOptions options) {
        CommandResult res = _db.command(new BasicDBObject("parallelCollectionScan", getName())
                                        .append("numCursors", options.getNumCursors()),
                                        options.getReadPreference());
        res.throwOnError();

        List<Cursor> cursors = new ArrayList<Cursor>();
        for (DBObject cursorDocument : (List<DBObject>) res.get("cursors")) {
            cursors.add(new QueryResultIterator(cursorDocument, db, this, options.getBatchSize(), getDecoder(), res.getServerUsed()));
        }

        return cursors;
    }


    @Override
    BulkWriteResult executeBulkWriteOperation(final boolean ordered, final List<WriteRequest> writeRequests,
                                              final WriteConcern writeConcern, DBEncoder encoder) {
        isTrue("no operations", !writeRequests.isEmpty());

        if (writeConcern == null) {
            throw new IllegalArgumentException("Write concern can not be null");
        }

        if (encoder == null) {
            encoder = DefaultDBEncoder.FACTORY.create();
        }

        DBPort port = db.getConnector().getPrimaryPort();
        try {
            BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(port.getAddress(), writeConcern);
            for (Run run : getRunGenerator(ordered, writeRequests, writeConcern, encoder, port)) {
                try {
                    BulkWriteResult result = run.execute(port);
                    if (result.isAcknowledged()) {
                        bulkWriteBatchCombiner.addResult(result, run.indexMap);
                    }
                } catch (BulkWriteException e) {
                    bulkWriteBatchCombiner.addErrorResult(e, run.indexMap);
                    if (bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {
                        break;
                    }
                }
            }
            return bulkWriteBatchCombiner.getResult();
        } finally {
            db.getConnector().releasePort(port);
        }
    }

    public WriteResult insert(List<DBObject> list, WriteConcern concern, DBEncoder encoder ){
        return insert(list, true, concern, encoder);
    }

    protected WriteResult insert(List<DBObject> list, boolean shouldApply , WriteConcern concern, DBEncoder encoder ){
        if (concern == null) {
            throw new IllegalArgumentException("Write concern can not be null");
        }

        if (encoder == null)
            encoder = DefaultDBEncoder.FACTORY.create();

        if ( willTrace() ) {
            for (DBObject o : list) {
                trace("save:  " + namespace + " " + JSON.serialize(o));
            }
        }

        DBPort port = db.getConnector().getPrimaryPort();
        try {
            if (useWriteCommands(concern, port)) {
                try {
                    return translateBulkWriteResult(insertWithCommandProtocol(list, concern, encoder, port, shouldApply), INSERT, concern,
                                                    port.getAddress());
                } catch (BulkWriteException e) {
                    throw translateBulkWriteException(e, INSERT);
                }
            }
            else {
                return insertWithWriteProtocol(list, concern, encoder, port, shouldApply);
            }
        } finally {
            db.getConnector().releasePort(port);
        }
    }

    public WriteResult remove( DBObject query, WriteConcern concern, DBEncoder encoder ) {
       return remove(query, true, concern, encoder);
    }

    public WriteResult remove( DBObject query, boolean multi, WriteConcern concern, DBEncoder encoder ){
        if (concern == null) {
            throw new IllegalArgumentException("Write concern can not be null");
        }

        if (encoder == null) {
            encoder = DefaultDBEncoder.FACTORY.create();
        }

        if (willTrace()) {
            trace("remove: " + namespace + " " + JSON.serialize(query));
        }

        DBPort port = db.getConnector().getPrimaryPort();
        try {
            if (useWriteCommands(concern, port)) {
                try {
                    return translateBulkWriteResult(removeWithCommandProtocol(Arrays.asList(new RemoveRequest(query, multi)),
                                                                              concern,
                                                                              encoder,
                                                                              port), REMOVE, concern, port.getAddress());
                } catch (BulkWriteException e) {
                    throw translateBulkWriteException(e, REMOVE);
                }
            }
            else {
                return db.getConnector().say(_db, OutMessage.remove(this, encoder, query, multi), concern, port);
            }
        } finally {
            db.getConnector().releasePort(port);
        }
    }

    @Override
    public WriteResult update( DBObject query , DBObject o , boolean upsert , boolean multi , WriteConcern concern,
                               DBEncoder encoder ) {

        if (o == null) {
            throw new IllegalArgumentException("update can not be null");
        }

        if (concern == null) {
            throw new IllegalArgumentException("Write concern can not be null");
        }

        if (encoder == null)
            encoder = DefaultDBEncoder.FACTORY.create();

        if (!o.keySet().isEmpty()) {
            // if 1st key doesn't start with $, then object will be inserted as is, need to check it
            String key = o.keySet().iterator().next();
            if (!key.startsWith("$"))
                _checkObject(o, false, false);
        }

        if ( willTrace() ) {
            trace("update: " + namespace + " " + JSON.serialize(query) + " " + JSON.serialize(o));
        }

        DBPort port = db.getConnector().getPrimaryPort();
        try {
            if (useWriteCommands(concern, port)) {
                try {
                    BulkWriteResult bulkWriteResult =
                    updateWithCommandProtocol(Arrays.<ModifyRequest>asList(new UpdateRequest(query, upsert, o, multi)),
                                              concern, encoder, port);
                    return translateBulkWriteResult(bulkWriteResult, UPDATE, concern, port.getAddress());
                } catch (BulkWriteException e) {
                    throw translateBulkWriteException(e, UPDATE);
                }
            } else {
                return db.getConnector().say(_db, OutMessage.update(this, encoder, upsert, multi, query, o), concern, port);
            }
        } finally {
            db.getConnector().releasePort(port);
        }
    }

    @Override
    public void drop(){
        db._collections.remove(getName());
        super.drop();
    }

    public void doapply( DBObject o ){
    }

    private WriteResult translateBulkWriteResult(final BulkWriteResult bulkWriteResult, final WriteRequest.Type type,
                                                 final WriteConcern writeConcern, final ServerAddress serverAddress) {
        CommandResult commandResult = new CommandResult(serverAddress);
        addBulkWriteResultToCommandResult(bulkWriteResult, type, commandResult);
        return new WriteResult(commandResult, writeConcern);
    }

    private MongoException translateBulkWriteException(final BulkWriteException e, final WriteRequest.Type type) {
        BulkWriteError lastError = e.getWriteErrors().isEmpty() ? null : e.getWriteErrors().get(e.getWriteErrors().size() - 1);
        CommandResult commandResult = new CommandResult(e.getServerAddress());
        addBulkWriteResultToCommandResult(e.getWriteResult(), type, commandResult);
        if (e.getWriteConcernError() != null) {
            commandResult.putAll(e.getWriteConcernError().getDetails());
        }

        if (lastError != null) {
            commandResult.put("err", lastError.getMessage());
            commandResult.put("code", lastError.getCode());
            commandResult.putAll(lastError.getDetails());
        } else if (e.getWriteConcernError() != null) {
            commandResult.put("err", e.getWriteConcernError().getMessage());
            commandResult.put("code", e.getWriteConcernError().getCode());
        }
        return commandResult.getException();
    }

    private void addBulkWriteResultToCommandResult(final BulkWriteResult bulkWriteResult, final WriteRequest.Type type,
                                                   final CommandResult commandResult) {
        commandResult.put("ok", 1);
        if (type == INSERT) {
           commandResult.put("n", 0);
        } else if (type == REMOVE) {
           commandResult.put("n", bulkWriteResult.getRemovedCount());
        } else if (type == UPDATE || type == REPLACE) {
            commandResult.put("n", bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size());
            if (bulkWriteResult.getMatchedCount() > 0) {
                commandResult.put("updatedExisting", true);
            } else {
                commandResult.put("updatedExisting", false);
            }
            if (!bulkWriteResult.getUpserts().isEmpty()) {
                commandResult.put("upserted", bulkWriteResult.getUpserts().get(0).getId());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public List<DBObject> getIndexInfo() {
        DBTCPConnector connector = db.getConnector();
        DBPort port = db.getConnector().getPrimaryPort();
        List<DBObject> list = new ArrayList<DBObject>();

        if (connector.getServerDescription(port.getAddress()).getVersion().compareTo(new ServerVersion(asList(2, 7, 7))) >= 0) {
            CommandResult res = _db.command(new BasicDBObject("listIndexes", getName()));
            if (!res.ok() && res.getCode() == 26) {
                return list;
            }
            res.throwOnError();
            for (DBObject indexDocument : (List<DBObject>) res.get("indexes")) {
                list.add(indexDocument);
            }
        } else {
            BasicDBObject cmd = new BasicDBObject("ns", getFullName());
            DBCursor cur = _db.getCollection("system.indexes").find(cmd);
            while (cur.hasNext()) {
                list.add(cur.next());
            }
        }

        return list;
    }

    public void createIndex(final DBObject keys, final DBObject options, DBEncoder encoder) {
        DBTCPConnector connector = db.getConnector();
        final DBPort port = db.getConnector().getPrimaryPort();

        try {
            DBObject index = defaultOptions(keys);
            index.putAll(options);
            index.put("key", keys);

            if (connector.getServerDescription(port.getAddress()).getVersion().compareTo(new ServerVersion(2, 6)) >= 0) {
                final BasicDBObject createIndexes = new BasicDBObject("createIndexes", getName());

                BasicDBList list = new BasicDBList();
                list.add(index);
                createIndexes.put("indexes", list);

                CommandResult commandResult = connector.doOperation(db, port, new DBPort.Operation<CommandResult>() {
                    @Override
                    public CommandResult execute() throws IOException {
                       return port.runCommand(db, createIndexes);
                    }
                });
                try {
                    commandResult.throwOnError();
                } catch (CommandFailureException e) {
                    if(e.getCode() == 11000) {
                        throw new MongoException.DuplicateKey(commandResult);
                    } else { 
                        throw e;
                    }
                }
            } else {
                db.doGetCollection("system.indexes").insertWithWriteProtocol(asList(index), WriteConcern.SAFE,
                                                                             DefaultDBEncoder.FACTORY.create(), port, false);
            }
        } finally {
            connector.releasePort(port);
        }
    }

    private BulkWriteResult insertWithCommandProtocol(final List<DBObject> list, final WriteConcern writeConcern,
                                                      final DBEncoder encoder,
                                                      final DBPort port, final boolean shouldApply) {
        if ( shouldApply ){
            applyRulesForInsert(list);
        }

        BaseWriteCommandMessage message = new InsertCommandMessage(getNamespace(), writeConcern, list,
                                                                   DefaultDBEncoder.FACTORY.create(), encoder,
                                                                   getMessageSettings(port));
        return writeWithCommandProtocol(port, INSERT, message, writeConcern);
    }

    private void applyRulesForInsert(final List<DBObject> list) {
        for (DBObject o : list) {
            _checkObject(o, false, false);
            apply(o);
            Object id = o.get("_id");
            if (id instanceof ObjectId) {
                ((ObjectId) id).notNew();
            }
        }
    }

    private BulkWriteResult removeWithCommandProtocol(final List<RemoveRequest> removeList,
                                                      final WriteConcern writeConcern,
                                                      final DBEncoder encoder, final DBPort port) {
        BaseWriteCommandMessage message = new DeleteCommandMessage(getNamespace(), writeConcern, removeList,
                                                                   DefaultDBEncoder.FACTORY.create(), encoder,
                                                                   getMessageSettings(port));
        return writeWithCommandProtocol(port, REMOVE, message, writeConcern);
    }

    @SuppressWarnings("unchecked")
    private BulkWriteResult updateWithCommandProtocol(final List<ModifyRequest> updates,
                                                      final WriteConcern writeConcern,
                                                      final DBEncoder encoder, final DBPort port) {
        BaseWriteCommandMessage message = new UpdateCommandMessage(getNamespace(), writeConcern, updates,
                                                                   DefaultDBEncoder.FACTORY.create(), encoder,
                                                                   getMessageSettings(port));
        return writeWithCommandProtocol(port, UPDATE, message, writeConcern);
    }

    private BulkWriteResult writeWithCommandProtocol(final DBPort port, final WriteRequest.Type type, BaseWriteCommandMessage message,
                                                     final WriteConcern writeConcern) {
        int batchNum = 0;
        int currentRangeStartIndex = 0;
        BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(port.getAddress(), writeConcern);
        do {
            batchNum++;
            BaseWriteCommandMessage nextMessage = sendWriteCommandMessage(message, batchNum, port);
            int itemCount = nextMessage != null ? message.getItemCount() - nextMessage.getItemCount() : message.getItemCount();
            IndexMap indexMap = IndexMap.create(currentRangeStartIndex, itemCount);
            CommandResult commandResult = receiveWriteCommandMessage(port);
            if (willTrace() && nextMessage != null || batchNum > 1) {
                getLogger().fine(format("Received response for batch %d", batchNum));
            }

            if (hasError(commandResult)) {
                bulkWriteBatchCombiner.addErrorResult(getBulkWriteException(type, commandResult), indexMap);
            } else {
                bulkWriteBatchCombiner.addResult(getBulkWriteResult(type, commandResult), indexMap);
            }
            currentRangeStartIndex += itemCount;
            message = nextMessage;
        } while (message != null && !bulkWriteBatchCombiner.shouldStopSendingMoreBatches());

        return bulkWriteBatchCombiner.getResult();
    }

    private boolean useWriteCommands(final WriteConcern concern, final DBPort port) {
        return concern.callGetLastError() &&
               db.getConnector().getServerDescription(port.getAddress()).getVersion().compareTo(new ServerVersion(2, 6)) >= 0;
    }

    private MessageSettings getMessageSettings(final DBPort port) {
        ServerDescription serverDescription = db.getConnector().getServerDescription(port.getAddress());
        return MessageSettings.builder()
                              .maxDocumentSize(serverDescription.getMaxDocumentSize())
                              .maxMessageSize(serverDescription.getMaxMessageSize())
                              .maxWriteBatchSize(serverDescription.getMaxWriteBatchSize())
                              .build();
    }

    private int getMaxWriteBatchSize(final DBPort port) {
        return db.getConnector().getServerDescription(port.getAddress()).getMaxWriteBatchSize();
    }

    private MongoNamespace getNamespace() {
        return new MongoNamespace(getDB().getName(), getName());
    }

    private BaseWriteCommandMessage sendWriteCommandMessage(final BaseWriteCommandMessage message, final int batchNum,
                                                            final DBPort port) {
        final PoolOutputBuffer buffer = new PoolOutputBuffer();
        try {
            BaseWriteCommandMessage nextMessage = message.encode(buffer);
            if (nextMessage != null || batchNum > 1) {
                getLogger().fine(format("Sending batch %d", batchNum));
            }
            db.getConnector().doOperation(getDB(), port, new DBPort.Operation<Void>() {
                @Override
                public Void execute() throws IOException {
                    buffer.pipe(port.getOutputStream());
                    return null;
                }
            });
            return nextMessage;
        } finally {
            buffer.reset();
        }
    }

    private CommandResult receiveWriteCommandMessage(final DBPort port) {
        return db.getConnector().doOperation(getDB(), port, new DBPort.Operation<CommandResult>() {
            @Override
            public CommandResult execute() throws IOException {
                Response response = new Response(port.getAddress(), null, port.getInputStream(),
                                                 DefaultDBDecoder.FACTORY.create());
                CommandResult writeCommandResult = new CommandResult(port.getAddress());
                writeCommandResult.putAll(response.get(0));
                writeCommandResult.throwOnError();
                return writeCommandResult;
            }
        });
    }


    private WriteResult insertWithWriteProtocol(final List<DBObject> list, final WriteConcern concern, final DBEncoder encoder,
                                                final DBPort port, final boolean shouldApply) {
        if ( shouldApply ){
            applyRulesForInsert(list);
        }

        WriteResult last = null;

        int cur = 0;
        int maxsize = db._mongo.getMaxBsonObjectSize();
        while ( cur < list.size() ) {

            OutMessage om = OutMessage.insert( this , encoder, concern );

            for ( ; cur < list.size(); cur++ ){
                DBObject o = list.get(cur);
                om.putObject( o );

                // limit for batch insert is 4 x maxbson on server, use 2 x to be safe
                if ( om.size() > 2 * maxsize ){
                    cur++;
                    break;
                }
            }

            last = db.getConnector().say(_db, om, concern, port);
        }

        return last;
    }

    private Iterable<Run> getRunGenerator(final boolean ordered, final List<WriteRequest> writeRequests,
                                          final WriteConcern writeConcern, final DBEncoder encoder, final DBPort port) {
        if (ordered) {
            return new OrderedRunGenerator(writeRequests, writeConcern, encoder, port);
        } else {
            return new UnorderedRunGenerator(writeRequests, writeConcern, encoder, port);
        }
    }

    private static final Logger TRACE_LOGGER = Logger.getLogger( "com.mongodb.TRACE" );
    private static final Level TRACE_LEVEL = Boolean.getBoolean( "DB.TRACE" ) ? Level.INFO : Level.FINEST;

    private boolean willTrace(){
        return TRACE_LOGGER.isLoggable(TRACE_LEVEL);
    }

    private void trace( String s ){
        TRACE_LOGGER.log( TRACE_LEVEL , s );
    }

    private Logger getLogger() {
        return TRACE_LOGGER;
    }

    private class OrderedRunGenerator implements Iterable<Run> {
        private final List<WriteRequest> writeRequests;
        private final WriteConcern writeConcern;
        private final DBEncoder encoder;
        private final int maxBatchWriteSize;

        public OrderedRunGenerator(final List<WriteRequest> writeRequests, final WriteConcern writeConcern, final DBEncoder encoder,
                                   final DBPort port) {
            this.writeRequests = writeRequests;
            this.writeConcern = writeConcern.continueOnError(false);
            this.encoder = encoder;
            this.maxBatchWriteSize = getMaxWriteBatchSize(port);
        }

        @Override
        public Iterator<Run> iterator() {
            return new Iterator<Run>() {
                private int curIndex;

                @Override
                public boolean hasNext() {
                    return curIndex < writeRequests.size();
                }

                @Override
                public Run next() {
                    Run run = new Run(writeRequests.get(curIndex).getType(), writeConcern, encoder);
                    int startIndexOfNextRun = getStartIndexOfNextRun();
                    for (int i = curIndex; i < startIndexOfNextRun; i++) {
                        run.add(writeRequests.get(i), i);
                    }
                    curIndex = startIndexOfNextRun;
                    return run;
                }

                private int getStartIndexOfNextRun() {
                    WriteRequest.Type type = writeRequests.get(curIndex).getType();
                    for (int i = curIndex; i < writeRequests.size(); i++) {
                        if (i == curIndex + maxBatchWriteSize || writeRequests.get(i).getType() != type) {
                            return i;
                        }
                    }
                    return writeRequests.size();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not implemented");
                }
            };
        }
    }


    private class UnorderedRunGenerator implements Iterable<Run> {
        private final List<WriteRequest> writeRequests;
        private final WriteConcern writeConcern;
        private final DBEncoder encoder;
        private final int maxBatchWriteSize;

        public UnorderedRunGenerator(final List<WriteRequest> writeRequests, final WriteConcern writeConcern,
                                     final DBEncoder encoder, final DBPort port) {
            this.writeRequests = writeRequests;
            this.writeConcern = writeConcern.continueOnError(true);
            this.encoder = encoder;
            this.maxBatchWriteSize = getMaxWriteBatchSize(port);
        }

        @Override
        public Iterator<Run> iterator() {
            return new Iterator<Run>() {
                private final Map<WriteRequest.Type, Run> runs =
                new TreeMap<WriteRequest.Type, Run>(new Comparator<WriteRequest.Type>() {
                    @Override
                    public int compare(final WriteRequest.Type first, final WriteRequest.Type second) {
                        return first.compareTo(second);
                    }
                });
                private int curIndex;

                @Override
                public boolean hasNext() {
                    return curIndex < writeRequests.size() || !runs.isEmpty();
                }

                @Override
                public Run next() {
                    while (curIndex < writeRequests.size()) {
                        WriteRequest writeRequest = writeRequests.get(curIndex);
                        Run run = runs.get(writeRequest.getType());
                        if (run == null) {
                            run = new Run(writeRequest.getType(), writeConcern, encoder);
                            runs.put(run.type, run);
                        }
                        run.add(writeRequest, curIndex);
                        curIndex++;
                        if (run.size() == maxBatchWriteSize) {
                            return runs.remove(run.type);
                        }
                    }

                    return runs.remove(runs.keySet().iterator().next());
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("Not implemented");
                }
            };
        }
    }

    private class Run {
        private final List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
        private final WriteRequest.Type type;
        private final WriteConcern writeConcern;
        private final DBEncoder encoder;
        private IndexMap indexMap;

        Run(final WriteRequest.Type type, final WriteConcern writeConcern, final DBEncoder encoder) {
            this.type = type;
            this.indexMap = IndexMap.create();
            this.writeConcern = writeConcern;
            this.encoder = encoder;
        }

        @SuppressWarnings("unchecked")
        void add(final WriteRequest writeRequest, final int originalIndex) {
            indexMap = indexMap.add(writeRequests.size(), originalIndex);
            writeRequests.add(writeRequest);
        }

        public int size() {
            return writeRequests.size();
        }

        @SuppressWarnings("unchecked")
        BulkWriteResult execute(final DBPort port) {
            if (type == UPDATE) {
                return executeUpdates(getWriteRequestsAsModifyRequests(), port);
            } else if (type == REPLACE) {
                return executeReplaces(getWriteRequestsAsModifyRequests(), port);
            } else if (type == INSERT) {
                return executeInserts(getWriteRequestsAsInsertRequests(), port);
            } else if (type == REMOVE) {
                return executeRemoves(getWriteRequestsAsRemoveRequests(), port);
            } else {
                throw new MongoInternalException(format("Unsupported write of type %s", type));
            }
        }

        private List getWriteRequestsAsRaw() {
            return writeRequests;
        }

        @SuppressWarnings("unchecked")
        private List<RemoveRequest> getWriteRequestsAsRemoveRequests() {
            return (List<RemoveRequest>) getWriteRequestsAsRaw();
        }

        @SuppressWarnings("unchecked")
        private List<InsertRequest> getWriteRequestsAsInsertRequests() {
            return (List<InsertRequest>) getWriteRequestsAsRaw();
        }

        @SuppressWarnings("unchecked")
        private List<ModifyRequest> getWriteRequestsAsModifyRequests() {
            return (List<ModifyRequest>) getWriteRequestsAsRaw();
        }

        BulkWriteResult executeUpdates(final List<ModifyRequest> updateRequests, final DBPort port) {
            for (ModifyRequest request : updateRequests) {
                for (String key : request.getUpdateDocument().keySet()) {
                    if (!key.startsWith("$")) {
                        throw new IllegalArgumentException("Update document keys must start with $: " + key);
                    }
                }
            }

            return new RunExecutor(port) {
                @Override
                BulkWriteResult executeWriteCommandProtocol() {
                    return updateWithCommandProtocol(updateRequests, writeConcern, encoder, port);
                }

                @Override
                WriteResult executeWriteProtocol(final int i) {
                    ModifyRequest update = updateRequests.get(i);
                    WriteResult writeResult = update(update.getQuery(), update.getUpdateDocument(), update.isUpsert(),
                            update.isMulti(), writeConcern, encoder);
                    return addMissingUpserted(update, writeResult);
                }

                @Override
                WriteRequest.Type getType() {
                    return UPDATE;
                }
            }.execute();
        }

        BulkWriteResult executeReplaces(final List<ModifyRequest> replaceRequests, final DBPort port) {
            for (ModifyRequest request : replaceRequests) {
                _checkObject(request.getUpdateDocument(), false, false);
            }

            return new RunExecutor(port) {
                @Override
                BulkWriteResult executeWriteCommandProtocol() {
                    return updateWithCommandProtocol(replaceRequests, writeConcern, encoder, port);
                }

                @Override
                WriteResult executeWriteProtocol(final int i) {
                    ModifyRequest update = replaceRequests.get(i);
                    WriteResult writeResult = update(update.getQuery(), update.getUpdateDocument(), update.isUpsert(),
                            update.isMulti(), writeConcern, encoder);
                    return addMissingUpserted(update, writeResult);
                }

                @Override
                WriteRequest.Type getType() {
                    return REPLACE;
                }
            }.execute();
        }

        BulkWriteResult executeRemoves(final List<RemoveRequest> removeRequests, final DBPort port) {
            return new RunExecutor(port) {
                @Override
                BulkWriteResult executeWriteCommandProtocol() {
                    return removeWithCommandProtocol(removeRequests, writeConcern, encoder, port);
                }

                @Override
                WriteResult executeWriteProtocol(final int i) {
                    RemoveRequest removeRequest = removeRequests.get(i);
                    return remove(removeRequest.getQuery(), removeRequest.isMulti(), writeConcern, encoder);
                }

                @Override
                WriteRequest.Type getType() {
                    return REMOVE;
                }
            }.execute();
        }

        BulkWriteResult executeInserts(final List<InsertRequest> insertRequests, final DBPort port) {
            return new RunExecutor(port) {
                @Override
                BulkWriteResult executeWriteCommandProtocol() {
                    List<DBObject> documents = new ArrayList<DBObject>(insertRequests.size());
                    for (InsertRequest cur : insertRequests) {
                        documents.add(cur.getDocument());
                    }
                    return insertWithCommandProtocol(documents, writeConcern, encoder, port, true);
                }

                @Override
                WriteResult executeWriteProtocol(final int i) {
                    return insert(asList(insertRequests.get(i).getDocument()), writeConcern, encoder);
                }

                @Override
                WriteRequest.Type getType() {
                    return INSERT;
                }

            }.execute();
        }


        private abstract class RunExecutor {
            private final DBPort port;

            RunExecutor(final DBPort port) {
                this.port = port;
            }

            abstract BulkWriteResult executeWriteCommandProtocol();

            abstract WriteResult executeWriteProtocol(final int i);

            abstract WriteRequest.Type getType();

            BulkWriteResult getResult(final WriteResult writeResult) {
                int count = getCount(writeResult);
                List<BulkWriteUpsert> upsertedItems = getUpsertedItems(writeResult);
                Integer modifiedCount = (getType() == UPDATE || getType() == REPLACE) ? null : 0;
                return new AcknowledgedBulkWriteResult(getType(), count - upsertedItems.size(), modifiedCount, upsertedItems);
            }

            BulkWriteResult execute() {
                if (useWriteCommands(writeConcern, port)) {
                    return executeWriteCommandProtocol();
                } else {
                    return executeWriteProtocol();
                }
            }

            private BulkWriteResult executeWriteProtocol() {
                BulkWriteBatchCombiner bulkWriteBatchCombiner = new BulkWriteBatchCombiner(port.getAddress(), writeConcern);
                for (int i = 0; i < writeRequests.size(); i++) {
                    IndexMap indexMap = IndexMap.create(i, 1);
                    try {
                        WriteResult writeResult = executeWriteProtocol(i);
                        if (writeConcern.callGetLastError()) {
                            bulkWriteBatchCombiner.addResult(getResult(writeResult), indexMap);
                        }
                    } catch (WriteConcernException writeException) {
                        if (isWriteConcernError(writeException.getCommandResult()))  {
                            bulkWriteBatchCombiner.addResult(getResult(new WriteResult(writeException.getCommandResult(), writeConcern)),
                                                             indexMap);
                            bulkWriteBatchCombiner.addWriteConcernErrorResult(getWriteConcernError(writeException.getCommandResult()));
                        } else {
                            bulkWriteBatchCombiner.addWriteErrorResult(getBulkWriteError(writeException), indexMap);
                        }
                        if (bulkWriteBatchCombiner.shouldStopSendingMoreBatches()) {
                            break;
                        }
                    }
                }
                return bulkWriteBatchCombiner.getResult();
            }


            private int getCount(final WriteResult writeResult) {
                return getType() == INSERT ? 1 : writeResult.getN();
            }

            List<BulkWriteUpsert> getUpsertedItems(final WriteResult writeResult) {
                return writeResult.getUpsertedId() == null
                       ? Collections.<BulkWriteUpsert>emptyList()
                       : asList(new BulkWriteUpsert(0, writeResult.getUpsertedId()));
            }

            private BulkWriteError getBulkWriteError(final WriteConcernException writeException) {
                return new BulkWriteError(writeException.getCode(),
                                          writeException.getCommandResult().getString("err"),
                                          getErrorResponseDetails(writeException.getCommandResult()),
                                          0);
            }

            // Accommodating GLE representation of write concern errors
            private boolean isWriteConcernError(final CommandResult commandResult) {
                return commandResult.get("wtimeout") != null;
            }

            private WriteConcernError getWriteConcernError(final CommandResult commandResult) {
                return new WriteConcernError(commandResult.getCode(), getWriteConcernErrorMessage(commandResult),
                                             getErrorResponseDetails(commandResult));
            }

            private String getWriteConcernErrorMessage(final CommandResult commandResult) {
                return commandResult.getString("err");
            }

            private DBObject getErrorResponseDetails(final DBObject response) {
                DBObject details = new BasicDBObject();
                for (String key : response.keySet()) {
                    if (!asList("ok", "err", "code").contains(key)) {
                        details.put(key, response.get(key));
                    }
                }
                return details;
            }

            WriteResult addMissingUpserted(final ModifyRequest update, final WriteResult writeResult) {
                // On pre 2.6 servers upserts with custom _id's would be not be reported so we  check if _id
                // was in the update query or the find query then massage the writeResult.
                if (update.isUpsert() && writeConcern.callGetLastError() && !writeResult.isUpdateOfExisting()
                        && writeResult.getUpsertedId() == null) {
                    DBObject updateDocument = update.getUpdateDocument();
                    DBObject query = update.getQuery();
                    if (updateDocument.containsField("_id")) {
                        CommandResult commandResult = writeResult.getLastError();
                        commandResult.put("upserted", updateDocument.get("_id"));
                        return new WriteResult(commandResult, writeResult.getLastConcern());
                    } else  if (query.containsField("_id")) {
                        CommandResult commandResult = writeResult.getLastError();
                        commandResult.put("upserted", query.get("_id"));
                        return new WriteResult(commandResult, writeResult.getLastConcern());
                    }
                }
                return writeResult;
            }
        }
    }
}
