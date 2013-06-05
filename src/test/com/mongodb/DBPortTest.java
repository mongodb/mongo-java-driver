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
 *
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

public class DBPortTest extends TestCase {
    @Test
    @SuppressWarnings("deprecation")
    public void testAuthentication() throws IOException {
        Mongo m = new MongoClient();
        DB db1 = m.getDB("DBPortTest1");
        DB db2 = m.getDB("DBPortTest2");
        db1.dropDatabase();
        db2.dropDatabase();

        try {
            db1.addUser("u1", "e".toCharArray());
            db2.addUser("u2", "e".toCharArray());

            DBPort port = new DBPort(m.getAddress(), new DBPortPool(m.getAddress(), new MongoOptions()), new MongoOptions());
            port.checkAuth(m);

            Set<String> expected = new HashSet<String>();

            assertEquals(expected, port.authenticatedDatabases);

            m.getAuthority().getCredentialsStore().add(MongoCredential.createMongoCRCredential("u1", "DBPortTest1", "e".toCharArray()));
            m.getAuthority().getCredentialsStore().add(MongoCredential.createMongoCRCredential("u2", "DBPortTest2", "e".toCharArray()));

            port.checkAuth(m);

            expected.add("DBPortTest1");
            expected.add("DBPortTest2");
            assertEquals(expected, port.authenticatedDatabases);

            m.getAuthority().getCredentialsStore().add(MongoCredential.createMongoCRCredential("u2", "DBPortTest3", "e".toCharArray()));

            try {
                port.checkAuth(m);
                fail("should throw");
            } catch (CommandFailureException e) {
                // all good
            }
        }
        finally {
            m.close();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testOpenFailure() throws UnknownHostException {
        final MongoOptions options = new MongoOptions();
        options.autoConnectRetry = true;
        options.maxAutoConnectRetryTime = 350;

        final DBPortPool portPool = new DBPortPool(new ServerAddress("localhost", 50051), options);
        portPool._everWorked = true;

        DBPort port = new DBPort(new ServerAddress("localhost", 50051), portPool, options);
        try {
            port._open();
            fail("Open should fail");
        } catch (IOException e) {
            // should get exception
        }

    }

}
