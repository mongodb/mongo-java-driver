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

package org.mongodb.io;

import org.mongodb.MongoConnector;
import org.mongodb.MongoCredential;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.MongoCommand;
import org.mongodb.result.CommandResult;

public class NativeAuthenticator extends Authenticator {
    NativeAuthenticator(final MongoCredential credential, final MongoConnector connector) {
        super(credential, connector);
    }

    @Override
    public CommandResult authenticate() {
        CommandResult nonceResponse = getConnector().command(getCredential().getSource(),
                new MongoCommand(NativeAuthenticationHelper.getNonceCommand()),
                        new DocumentCodec(PrimitiveCodecs.createDefault()));
        return getConnector().command(getCredential().getSource(),
                new MongoCommand(NativeAuthenticationHelper.getAuthCommand(getCredential().getUserName(),
                    getCredential().getPassword(), (String) nonceResponse.getResponse().get("nonce"))),
                new DocumentCodec(PrimitiveCodecs.createDefault()));
    }
}