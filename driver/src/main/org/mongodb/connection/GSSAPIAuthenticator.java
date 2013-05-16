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
import org.mongodb.MongoException;

import javax.security.sasl.SaslClient;
import java.nio.ByteBuffer;

class GSSAPIAuthenticator extends SaslAuthenticator {
    GSSAPIAuthenticator(final MongoCredential credential, final Connection connection, final BufferPool<ByteBuffer> bufferPool) {
        super(credential, connection, bufferPool);

        if (!this.getCredential().getMechanism().equals(MongoCredential.GSSAPI_MECHANISM)) {
            throw new MongoException("Incorrect mechanism: " + this.getCredential().getMechanism());
        }
    }

    @Override
    protected SaslClient createSaslClient() {
        return GSSAPIAuthenticationHelper.createSaslClient(getCredential(), getConnection().getServerAddress().getHost());
    }

    @Override
    public String getMechanismName() {
        return GSSAPIAuthenticationHelper.GSSAPI_MECHANISM_NAME;
    }
}
