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

package com.mongodb.jndi;

import com.mongodb.*;
import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import javax.naming.RefAddr;
import javax.naming.Reference;
import java.util.Arrays;
import java.util.List;

/**
 * Author: jmelo
 */
public class MongoJndiObjectFactoryBeanTest extends TestCase {

    public static final String URI_UNDER_TEST = "mongodb://user1:pwd@127.0.0.1:27017/test?connectTimeoutMS=150&socketTimeoutMS=300&maxPoolSize=500&readPreference=secondary";

    @Test
    public void testGetObjectInstanceValidURI() throws Exception {
        MongoJndiObjectFactoryBean bean = new MongoJndiObjectFactoryBean();
        MongoClientOptions customClientOptions = new MongoClientOptions.Builder().readPreference(ReadPreference.secondary()).connectTimeout(150).socketTimeout(300).connectionsPerHost(500).build();
        MongoOptions customOptions = new MongoOptions(customClientOptions);
        final List<MongoCredential> credentialsList = Arrays.asList(
                MongoCredential.createMongoCRCredential("user1", "test", "pwd".toCharArray()));


        final Reference obj = new Reference(MongoClient.class.getCanonicalName(), new RefAddr(MongoJndiObjectFactoryBean.MONGO_CLIENT_URI) {
            @Override
            public Object getContent() {
                return URI_UNDER_TEST;
            }
        });


        MongoClient mc = (MongoClient) bean.getObjectInstance(obj, null, null, null);
        assertNotNull(mc);
        assertEquals("secondary", mc.getReadPreference().getName());
        assertEquals(new ServerAddress("127.0.0.1"), mc.getAddress());
        assertEquals(customOptions, mc.getMongoOptions());
        assertEquals(credentialsList, mc.getCredentialsList());
        assertEquals(customClientOptions, mc.getMongoClientOptions());
        mc.close();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "uri needs to start with mongodb://")
    public void testGetObjectInstanceInvalidURI() throws Exception {
        MongoJndiObjectFactoryBean bean = new MongoJndiObjectFactoryBean();
        final Reference obj = new Reference(MongoClient.class.getCanonicalName(), new RefAddr(MongoJndiObjectFactoryBean.MONGO_CLIENT_URI) {
            @Override
            public Object getContent() {
                return "invalid";
            }
        });

        bean.getObjectInstance(obj, null, null, null);
    }


    @Test(expectedExceptions = MongoException.class)
    public void testGetObjectInstanceWithEmptyPropertyName() throws Exception {
        MongoJndiObjectFactoryBean bean = new MongoJndiObjectFactoryBean();
        final Reference obj = new Reference(MongoClient.class.getCanonicalName(), new RefAddr(null) {
            @Override
            public Object getContent() {
                return URI_UNDER_TEST;
            }
        });
        bean.getObjectInstance(obj, null, null, null);
    }
}
