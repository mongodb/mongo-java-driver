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

import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.codecs.DocumentCodec;

import java.util.Collections;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertNotNull;
import static org.mongodb.Fixture.getBufferProvider;

public class SSLNIOStreamTest {

    @Ignore
    @Test
    public void testIt() {
        InternalConnection internalConnection = new InternalStreamConnection(
                new SSLNIOStream(new ServerAddress(), getBufferProvider(), Executors.newFixedThreadPool(1)),
                Collections.<MongoCredential>emptyList(), getBufferProvider(), new NoOpConnectionListener());
        assertNotNull(internalConnection);
        CommandHelper.executeCommand("test", new Document("getlasterror", 1), new DocumentCodec(), internalConnection, getBufferProvider());
    }
}
