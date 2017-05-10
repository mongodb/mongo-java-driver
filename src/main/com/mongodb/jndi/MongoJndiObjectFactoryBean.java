//MongoJndiObjectFactoryBean.java
/**
 *      Copyright (C) 2013 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   Author:
 *   Juan Luis Melo
 *
 */


package com.mongodb.jndi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Mongo factory bean to help configuring Mongodb using Jndi.
 *
 * @author jmelo
 */
public class MongoJndiObjectFactoryBean implements ObjectFactory {

    public static final String MONGO_CLIENT_URI = "mongoClientURI";


    public Object getObjectInstance(Object obj, Name name, Context nameCtx, Hashtable<?, ?> environment) throws Exception {

        String mongoURI = null;
        Enumeration<RefAddr> props = ((Reference) obj).getAll();

        while (props.hasMoreElements()) {
            RefAddr addr = props.nextElement();
            if (addr != null) {
                if (MONGO_CLIENT_URI.equals(addr.getType())) {
                    mongoURI = (String) addr.getContent();
                    break;
                }
            }
        }

        if (mongoURI == null || mongoURI.isEmpty()) {
            throw new MongoException(MONGO_CLIENT_URI + " resource property is empty");
        }

        return new MongoClient(new MongoClientURI(mongoURI));
    }

}
