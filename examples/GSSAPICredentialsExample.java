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
import java.security.Security;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Example usage of Kerberos (GSSAPI) credentials.
 */
public class GSSAPICredentialsExample {

    // Steps:
    // 1. Install unlimited strength encryption jar files in jre/lib/security
    // (e.g. http://www.oracle.com/technetwork/java/javase/downloads/jce-6-download-429243.html)
    // 2. run kinit
    // 3. Set system properties, e.g.:
    //    -Djava.security.krb5.realm=10GEN.ME -Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.krb5.kdc=kdc.10gen.me
    // auth.login.defaultCallbackHandler=name of class that implements javax.security.auth.callback.CallbackHandler
    // You may also need to define realms and domain_realm entries in your krb5.conf file (in /etc by default)
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        // Set this property to avoid the default behavior where the program prompts on the command line
        // for username/password
        Security.setProperty("auth.login.defaultCallbackHandler", "DefaultSecurityCallbackHandler");

        MongoClient mongo = new MongoClient(
                new MongoClientAuthority(new ServerAddress("kdc.10gen.me"),
                        new MongoClientCredentials("dev1@10GEN.ME", MongoClientCredentials.GSSAPI_MECHANISM)),
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