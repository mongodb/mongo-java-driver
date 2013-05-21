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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoClientOptions;

import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mongodb.Fixture.getPrimary;

public class DefaultSingleServerClusterTest {
    private DefaultSingleServerCluster cluster;

    @Before
    public void setUp() throws UnknownHostException {
        cluster = new DefaultSingleServerCluster(getPrimary(), null, MongoClientOptions.builder().build(),
                new PowerOfTwoByteBufferPool(), new DefaultServerFactory());
    }

    @After
    public void tearDown() {
        cluster.close();
    }

    @Test
    public void shouldGetDescription() {
         assertNotNull(cluster.getDescription());
    }

    @Test
    public void shouldGetServerWithOkDescription() throws InterruptedException {
        Server server = cluster.getServer(new PrimaryServerSelector());
        assertTrue(server.getDescription().isOk());
     }


}
