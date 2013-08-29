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

package org.mongodb.connection.impl;

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.operation.AsyncCommandOperation;

import java.util.Collections;

import static org.mongodb.connection.ClusterConnectionMode.Single;
import static org.mongodb.connection.ClusterType.Unknown;
import static org.mongodb.connection.impl.NativeAuthenticationHelper.getAuthCommand;
import static org.mongodb.connection.impl.NativeAuthenticationHelper.getNonceCommand;

class NativeAsyncAuthenticator extends AsyncAuthenticator {
    NativeAsyncAuthenticator(final MongoCredential credential, final AsyncConnection connection,
                             final BufferProvider bufferProvider) {
        super(credential, connection, bufferProvider);
    }

    @Override
    public void authenticate(final SingleResultCallback<CommandResult> callback) {
        new AsyncCommandOperation(getCredential().getSource(), getNonceCommand(), null, new DocumentCodec(PrimitiveCodecs.createDefault()),
                new ClusterDescription(Single, Unknown, Collections.<ServerDescription>emptyList()),
                getBufferProvider())
                .execute(new ConnectingAsyncServerConnection(getConnection()))
                .register(new SingleResultCallback<CommandResult>() {
                    @Override
                    public void onResult(final CommandResult result, final MongoException e) {
                        if (e != null) {
                            callback.onResult(result, e);
                        }
                        else {
                            final Document command = getAuthCommand(getCredential().getUserName(),
                                                                    getCredential().getPassword(),
                                                                    (String) result.getResponse().get("nonce"));
                            new AsyncCommandOperation(getCredential().getSource(), command, null,
                                                      new DocumentCodec(PrimitiveCodecs.createDefault()),
                                                      new ClusterDescription(Single, Unknown, Collections.<ServerDescription>emptyList()),
                                                      getBufferProvider())
                                    .execute(new ConnectingAsyncServerConnection(getConnection()))
                                    .register(new SingleResultCallback<CommandResult>() {
                                        @Override
                                        public void onResult(final CommandResult result, final MongoException e) {
                                            callback.onResult(result, e);
                                        }
                                    });
                        }
                    }
                });
    }
}
