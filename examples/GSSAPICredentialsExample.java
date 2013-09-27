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
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.security.Security;
import java.util.Arrays;

/**
 * Example usage of Kerberos (GSSAPI) credentials.
 * <p>
 * Usage:
 * </p>
 * <pre>
 *     java GSSAPICredentialsExample server userName databaseName
 * </pre>
 */
public class GSSAPICredentialsExample {

    // Steps:
    // 1. Install unlimited strength encryption jar files in jre/lib/security
    // (e.g. http://www.oracle.com/technetwork/java/javase/downloads/jce-6-download-429243.html)
    // 2. run kinit
    // 3. Set system properties, e.g.:
    //    -Djava.security.krb5.realm=10GEN.ME -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.krb5.kdc=kdc.10gen.me
    // auth.login.defaultCallbackHandler=name of class that implements javax.security.auth.callback.CallbackHandler
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        // Set this property to avoid the default behavior where the program prompts on the command line for username/password
//        Security.setProperty("auth.login.defaultCallbackHandler", "DefaultSecurityCallbackHandler");

        String server = args[0];
        String user = args[1];
        String databaseName = args[2];

        System.out.println("javax.security.auth.useSubjectCredsOnly: " + System.getProperty("javax.security.auth.useSubjectCredsOnly"));
        System.out.println("java.security.krb5.realm: " + System.getProperty("java.security.krb5.realm"));
        System.out.println("java.security.krb5.kdc: " + System.getProperty("java.security.krb5.kdc"));
        System.out.println("auth.login.defaultCallbackHandler: " + Security.getProperty("auth.login.defaultCallbackHandler"));
        System.out.println("login.configuration.provider: " + Security.getProperty("login.configuration.provider"));
        System.out.println("java.security.auth.login.config: " + Security.getProperty("java.security.auth.login.config"));
        System.out.println("login.config.url.1: " + Security.getProperty("login.config.url.1"));
        System.out.println("login.config.url.2: " + Security.getProperty("login.config.url.2"));
        System.out.println("login.config.url.3: " + Security.getProperty("login.config.url.3"));

        System.out.println("server: " + server);
        System.out.println("user: " + user);
        System.out.println("database: " + databaseName);

        System.out.println();

        MongoClient mongoClient = new MongoClient(new ServerAddress(server),
                        Arrays.asList(MongoCredential.createGSSAPICredential(user).withMechanismProperty("SERVICE_NAME", "mongodb")),
                new MongoClientOptions.Builder().socketKeepAlive(true).socketTimeout(30000).build());
        DB testDB = mongoClient.getDB(databaseName);

        System.out.println("Insert result: " + testDB.getCollection("test").insert(new BasicDBObject()));
        System.out.println("Count: " + testDB.getCollection("test").count());
    }
}
