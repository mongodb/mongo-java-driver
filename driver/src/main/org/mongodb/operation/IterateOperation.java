/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.operation;

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.List;

//TODO: is this the right thing to do?
public class IterateOperation<T> extends BaseOperation<MongoCursor<T>> {
    private final MongoNamespace namespace;
    private final List<Document> pipeline;
    private final Codec<Document> commandCodec = new DocumentCodec();
    private final ReadPreference readPreference;

    /**
     * The constructor of this abstract class takes the fields that are required by all basic operations.
     *
     * @param namespace
     * @param pipeline
     * @param readPreference
     * @param bufferProvider the BufferProvider to use when reading or writing to the network
     * @param session        the current Session, which will give access to a connection to the MongoDB instance
     * @param closeSession   true if the session should be closed at the end of the execute method
     */
    public IterateOperation(final MongoNamespace namespace, final List<Document> pipeline, final ReadPreference readPreference,
                            final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.namespace = namespace;
        this.pipeline = pipeline;
        this.readPreference = readPreference;
    }

    /**
     * Returns a cursor allowing a user to iterate over the results
     *
     * @return T, the results of the execution
     */
    @Override
    public MongoCursor<T> execute() {
        ServerConnectionProvider provider = getSession().createServerConnectionProvider(getServerConnectionProviderOptions());
        Document document = new Document("aggregate", namespace.getCollectionName()).append("pipeline", pipeline);
        CommandResult commandResult = new CommandProtocol(namespace.getDatabaseName(), document, commandCodec, commandCodec,
                                                          getBufferProvider(), provider.getServerDescription(),
                                                          provider.getConnection(), true)
                                          .execute();

        return new SingleShotCursor<T>((Iterable<T>) commandResult.getResponse().get("result"));
    }

    private ServerConnectionProviderOptions getServerConnectionProviderOptions() {
        return new ServerConnectionProviderOptions(true, new ReadPreferenceServerSelector(readPreference));
    }
}
