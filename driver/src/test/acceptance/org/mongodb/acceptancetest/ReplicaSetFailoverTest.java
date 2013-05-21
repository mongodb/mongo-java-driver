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

package org.mongodb.acceptancetest;

import org.mongodb.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;
import org.mongodb.MongoCollection;
import org.mongodb.connection.ServerAddress;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public final class ReplicaSetFailoverTest {
    public static void main(final String[] args) throws UnknownHostException, InterruptedException, TimeoutException {
        MongoClient client = MongoClients.create(Arrays.asList(new ServerAddress("localhost:27019")));

        final MongoCollection<Document> collection = client.getDatabase("test").getCollection("ReplicaSetFailover");
        collection.tools().drop();
        collection.insert(new Document());

        for (int threadCount = 0; threadCount < 50; threadCount++) {
            final int threadNum = threadCount;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < Integer.MAX_VALUE; i++) {
                        try {
//                            System.out.println(threadNum + ":" + i + ": " + collection.one());
                        } catch (Exception e) {
                            System.err.print(threadNum + ":" + i + ": ");
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) { // NOPMD
                            // all good
                        }
                    }
                }
            }).start();
        }
    }

    private ReplicaSetFailoverTest() {

    }
}