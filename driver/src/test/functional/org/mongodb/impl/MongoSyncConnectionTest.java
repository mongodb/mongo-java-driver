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

package org.mongodb.impl;

import org.junit.Before;
import org.mongodb.Fixture;
import org.mongodb.ServerAddress;
import org.mongodb.io.PowerOfTwoByteBufferPool;
import org.mongodb.pool.SimplePool;

public class MongoSyncConnectionTest extends AbstractMongoConnectorTest {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        setConnection(new MongoSyncConnection(new ServerAddress(Fixture.getMongoClientURI().getHosts().get(0)),
                Fixture.getMongoClientURI().getCredentials(), new SimplePool<MongoConnection>("test", 1) {
            @Override
            protected MongoConnection createNew() {
                throw new UnsupportedOperationException();
            }
        }, new PowerOfTwoByteBufferPool(), Fixture.getMongoClientURI().getOptions()));
    }
}
