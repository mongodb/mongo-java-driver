/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package example;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import javax.net.ssl.SSLSocketFactory;
import java.net.UnknownHostException;
import java.util.Arrays;

public class X509CredentialsExample {
    public static void main(String[] args) throws UnknownHostException {
        String server = args[0];
        String user = "CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US";

        System.out.println("server: " + server);
        System.out.println("user: " + user);

        System.out.println();

        MongoClient mongoClient = new MongoClient(new ServerAddress(server),
                                                  Arrays.asList(MongoCredential.createMongoX509Credential(user)),
                                                  new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build());
        DB testDB = mongoClient.getDB("test");

        System.out.println("Count: " + testDB.getCollection("test").count());

        System.out.println("Insert result: " + testDB.getCollection("test").insert(new BasicDBObject()));

    }
}
