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

package org.mongodb.command;

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.Operation;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.Find;
import org.mongodb.operation.ReadPreferenceServerSelector;
import org.mongodb.operation.ServerConnectionProvider;
import org.mongodb.operation.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

public class CountOperation extends BaseCountOperation implements Operation<Long> {

    private final Session session;
    private final boolean closeSession;

    public CountOperation(final Find find, final MongoNamespace namespace, final Codec<Document> codec,
                          final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(find, namespace, codec, bufferProvider);
        this.session = session;
        this.closeSession = closeSession;
    }

    public Long execute() {
        try {
            ServerConnectionProvider serverConnectionProvider = session.createServerConnectionProvider(
                    new ServerConnectionProviderOptions(true, new ReadPreferenceServerSelector(getCount().getReadPreference())));
            return getCount(new CommandProtocol(getCount().getNamespace().getDatabaseName(), getCount(), getCodec(),
                    getBufferProvider(), serverConnectionProvider.getServerDescription(), serverConnectionProvider.getConnection(), true)
                    .execute());
        } finally {
            if (closeSession) {
                session.close();
            }
        }
    }
}
