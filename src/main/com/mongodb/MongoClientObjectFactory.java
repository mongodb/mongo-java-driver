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

import java.util.Enumeration;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Allows to configure a {@code MongoClient} as a JNDI resource. This class
 * fulfills the javax.naming.spi.ObjectFactory contract, thereby allowing the
 * mongodb driver to be used for configuring JNDI resources at the container
 * level.
 * 
 * Please refer to 
 * <a href="http://api.mongodb.org/java/2.11.2/com/mongodb/MongoClientURI.html">MongoClientURI documentation</a> 
 * for the various options that can be passed in via the URI.
 * 
 * For e.g. In an Apache Tomcat environment, you maybe able to define a
 * {@code <Resource>} element as shown below.
 * <pre>
 * 	<Resource name="mongodb/MongoClient" 
 * 		auth="Container"
 * 		type="com.mongodb.MongoClient"
 * 		factory="com.mongodb.MongoClientObjectFactory" 
 * 		mongoClientURI="mongodb://localhost:27017,some.other.host:28017,.../collectionName?safe=true;option1=someValue"/>
 * </pre>
 * 
 * In case you are using Spring for your application, you can refer to the mongoclient resource using the jee namespace support.
 * e.g
 * <pre>
 * <jee:jndi-lookup id="mongoClient" jndi-name="mongodb/MongoClient"/>
 * </pre>
 * and either use autowiring or setter/constructor injection to provide a reference to the mongoClient
 * 
 * <pre>
 * <bean id="mongoClientDependentBean" class="com.foo.bar.MongoRepo">
 * 	<property name="mongoClient" ref="mongoClient"/>
 *  ...
 *  ... 
 * </pre>
 * 
 * 
 * 
 * @see MongoClient
 * @since 2.12.0
 * @author jvictor
 * 
 */
public class MongoClientObjectFactory implements ObjectFactory {

	private static final Logger LOGGER = Logger
			.getLogger("com.mongodb.MongoClientObjectFactory");
	private static final String MONGO_CLIENT_URI = "mongoClientURI";

	public MongoClientObjectFactory() {
	}

	public Object getObjectInstance(Object object, Name name, Context nameCtx,
			Hashtable<?, ?> environment) throws Exception {

		String mongoURI = null;
		if (object != null) {
			Enumeration<RefAddr> clientProperties = ((Reference) object)
					.getAll();
			while (clientProperties.hasMoreElements()) {
				RefAddr addr = (RefAddr) clientProperties.nextElement();
				if (addr != null) {
					if (MONGO_CLIENT_URI.equals(addr.getType())) {
						mongoURI = (String) addr.getContent();
					}
				}
			}

			if (mongoURI == null || mongoURI.isEmpty()) {
				throw new MongoException(MONGO_CLIENT_URI
						+ " property is missing. " +
						"Please refer to http://api.mongodb.org/java/2.11.2/com/mongodb/MongoClientURI.html to configure");
			}
		} else {			
			return null;
		}		
		MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoURI));
		LOGGER.log(Level.INFO, "MongoClient configuration has completed.");
		LOGGER.log(Level.INFO, mongoClient.toString());
		return mongoClient;

	}

}
