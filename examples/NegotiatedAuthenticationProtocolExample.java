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
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredentials;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Example usage of NEGOTIATE authentication protocol.
 * <p>
 * Usage:
 * </p>
 * <pre>
 *     java NegotiatedAuthenticationProtocolExample server userName password databaseName
 * </pre>
 */
public class NegotiatedAuthenticationProtocolExample {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        String server = args[0];
        String user = args[1];
        String pwd = args[2];
        String db = args[3];

        MongoCredentials credentials = new MongoCredentials(user, pwd.toCharArray(), MongoCredentials.Protocol.NEGOTIATE, db);

        MongoClient mongoClient = new MongoClient(new ServerAddress(server), Arrays.asList(credentials), new MongoClientOptions.Builder().build());

        DB testDB = mongoClient.getDB(db);
        testDB.getCollection("test").insert(new BasicDBObject());
        System.out.println("Count: " + testDB.getCollection("test").count());
    }
}
