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

package org.mongodb.connection;

import org.mongodb.MongoCredential;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.MongoCommand;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.operation.CommandOperation;
import org.mongodb.operation.CommandResult;

import java.nio.ByteBuffer;

public class NativeAuthenticator extends Authenticator {
    private final BufferPool<ByteBuffer> bufferPool;

    public NativeAuthenticator(final MongoCredential credential, final Connection connection, final BufferPool<ByteBuffer> bufferPool) {
        super(credential, connection);
        this.bufferPool = bufferPool;
    }

    @Override
    public void authenticate() {
        try {
            CommandResult nonceResponse = new CommandOperation(getCredential().getSource(),
                    new MongoCommand(NativeAuthenticationHelper.getNonceCommand()),
                    new DocumentCodec(PrimitiveCodecs.createDefault()), bufferPool).execute(
                    new ConnectingServerConnection(getConnection()));
            new CommandOperation(getCredential().getSource(),
                    new MongoCommand(NativeAuthenticationHelper.getAuthCommand(getCredential().getUserName(),
                            getCredential().getPassword(), (String) nonceResponse.getResponse().get("nonce"))),
                    new DocumentCodec(PrimitiveCodecs.createDefault()), bufferPool).execute(
                    new ConnectingServerConnection(getConnection()));
        } catch (MongoCommandFailureException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating", e);
        }
    }
}
