/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class MongoClientTest {
    @Test
    @SuppressWarnings("deprecation")
    public void testConstructors() throws UnknownHostException {
        MongoClientOptions customClientOptions = new MongoClientOptions.Builder().connectionsPerHost(500).build();
        MongoOptions customOptions = new MongoOptions(customClientOptions);
        MongoOptions defaultOptions = new MongoOptions(new MongoClientOptions.Builder().build());
        List<MongoCredential> emptyCredentials = Arrays.asList();
        MongoClient mc;

        mc = new MongoClient();
        Assert.assertEquals(new ServerAddress(), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient("127.0.0.1");
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient("127.0.0.1", customClientOptions);
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient("127.0.0.1", 27018);
        Assert.assertEquals(new ServerAddress("127.0.0.1", 27018), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(new ServerAddress("127.0.0.1"));
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        final List<MongoCredential> credentialsList = Arrays.asList(
                MongoCredential.createMongoCRCredential("user1", "test", "pwd".toCharArray()));
        mc = new MongoClient(new ServerAddress("127.0.0.1"), credentialsList);
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(credentialsList, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(new ServerAddress("127.0.0.1"), customClientOptions);
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(new ServerAddress("127.0.0.1"), credentialsList, customClientOptions);
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(credentialsList, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)));
        Assert.assertEquals(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)), mc.getAllAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)), customClientOptions);
        Assert.assertEquals(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)), mc.getAllAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)), credentialsList, customClientOptions);
        Assert.assertEquals(Arrays.asList(new ServerAddress("localhost", 27017), new ServerAddress("127.0.0.1", 27018)), mc.getAllAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(credentialsList, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(new MongoClientURI("mongodb://127.0.0.1"));
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(defaultOptions, mc.getMongoOptions());
        Assert.assertEquals(emptyCredentials, mc.getCredentialsList());
        Assert.assertEquals(MongoClientOptions.builder().build(), mc.getMongoClientOptions());
        mc.close();

        mc = new MongoClient(new MongoClientURI("mongodb://user1:pwd@127.0.0.1/test?maxPoolSize=500"));
        Assert.assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        Assert.assertEquals(customOptions, mc.getMongoOptions());
        Assert.assertEquals(credentialsList, mc.getCredentialsList());
        Assert.assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();
    }
}
