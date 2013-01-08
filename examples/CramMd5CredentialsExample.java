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
import com.mongodb.MongoAuthority;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredentials;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import java.net.UnknownHostException;

/**
 * Example usage of CRAM-MD5 credentials.
 * <p>
 * Usage:
 * </p>
 * <pre>
 *     java CramMd5CredentialsExample server userName password databaseName
 * </pre>
 */
public class CramMd5CredentialsExample {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        String server = args[0];
        String user = args[1];
        String pwd = args[2];
        String db = args[3];
        MongoClient mongo = new MongoClient(
                new MongoAuthority(new ServerAddress(server),
                        new MongoCredentials(user, pwd.toCharArray(),
                                MongoCredentials.CRAM_MD5_MECHANISM, db)),
                new MongoClientOptions.Builder().build());
        DB testDB = mongo.getDB(db);
        System.out.println("Find one: " + testDB.getCollection("test").findOne());
        System.out.println("Count: " + testDB.getCollection("test").count());
        WriteResult writeResult = testDB.getCollection("test").insert(new BasicDBObject());
        System.out.println("Write result: " + writeResult);

        System.out.println();

        System.out.println("Count: " + testDB.getCollection("test").count());
    }
}
