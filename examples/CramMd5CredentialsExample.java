/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientAuthority;
import com.mongodb.MongoClientCredentials;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Example usage of CRAM-MD5 credentials.
 */
public class CramMd5CredentialsExample {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        MongoClient mongo = new MongoClient(
                new MongoClientAuthority(new ServerAddress("kdc.10gen.me"),
                        new MongoClientCredentials("dev0", "a".toCharArray(),
                                MongoClientCredentials.CRAM_MD5_MECHANISM, "test")),
                new MongoClientOptions.Builder().socketKeepAlive(true).socketTimeout(30000).build());
        DB testDB = mongo.getDB("test");
        System.out.println("Find     one: " + testDB.getCollection("test").findOne());
        System.out.println("Count: " + testDB.getCollection("test").count());
        WriteResult writeResult = testDB.getCollection("test").insert(new BasicDBObject());
        System.out.println("Write result: " + writeResult);

        System.out.println();
        System.out.println("Trying a query once every 15 seconds...");
        System.out.println();

        for (; ; ) {
            try {
                System.out.println(new SimpleDateFormat().format(new Date()));
                System.out.println("Count: " + testDB.getCollection("test").count());
                System.out.println();
            } catch (MongoException e) {
                e.printStackTrace();
                System.out.println();
            }
            Thread.sleep(15000);
        }
    }
}
