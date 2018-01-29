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

package com.mongodb.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.internal.ListDatabasesIterableImpl;
import com.mongodb.client.internal.MongoClientDelegate;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.connection.Cluster;
import com.mongodb.session.ClientSession;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

final class MongoClientImpl implements MongoClient {

    private final ReadPreference readPreference;
    private final boolean retryWrites;
    private final ReadConcern readConcern;
    private final WriteConcern writeConcern;
    private final CodecRegistry codecRegistry = MongoClients.getDefaultCodecRegistry();
    private final MongoClientDelegate delegate;

    MongoClientImpl(final Cluster cluster, final List<MongoCredential> credentialList, final ReadPreference readPreference,
                    final WriteConcern writeConcern, final boolean retryWrites, final ReadConcern readConcern) {
        this(cluster, credentialList, readPreference, writeConcern, retryWrites, readConcern, null);
    }

    MongoClientImpl(final Cluster cluster, final List<MongoCredential> credentialList, final ReadPreference readPreference,
                    final WriteConcern writeConcern, final boolean retryWrites, final ReadConcern readConcern,
                    final OperationExecutor operationExecutor) {
        this.readPreference = readPreference;
        this.retryWrites = retryWrites;
        this.readConcern = readConcern;
        this.writeConcern = writeConcern;
        this.delegate = new MongoClientDelegate(cluster, credentialList, this, operationExecutor);
    }

    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, codecRegistry, readPreference, writeConcern, retryWrites, readConcern,
                delegate.getOperationExecutor());
    }

    public MongoIterable<String> listDatabaseNames() {
        return createListDatabaseNamesIterable(null);
    }

    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListDatabaseNamesIterable(clientSession);
    }

    private MongoIterable<String> createListDatabaseNamesIterable(final ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }

    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
    }

    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> clazz) {
        return createListDatabasesIterable(null, clazz);
    }

    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    public <T> ListDatabasesIterable<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, clazz);
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(final ClientSession clientSession, final Class<T> clazz) {
        return new ListDatabasesIterableImpl<T>(clientSession, clazz, codecRegistry,
                ReadPreference.primary(), delegate.getOperationExecutor());
    }

    public ClientSession startSession(final ClientSessionOptions options) {
        ClientSession clientSession = delegate.createClientSession(notNull("options", options));
        if (clientSession == null) {
            throw new MongoClientException("Sessions are not supported by the MongoDB cluster to which this client is connected");
        }
        return clientSession;
    }

    @Override
    public void close() {
        delegate.close();
    }
}

