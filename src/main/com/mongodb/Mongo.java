// Mongo.java

/**
 *      Copyright (C) 2008 10gen Inc.
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
 */

package com.mongodb;

import java.net.UnknownHostException;

/**
 * A database connection and, optionally, database.
 * The following are equivalent, and all connect to the 
 * local database called "dev", running on the default port:
 *
 * <blockquote><pre>
 * Mongo mongo1 = new Mongo( "127.0.0.1", "dev" );
 * Mongo mongo2 = new Mongo( "127.0.0.1", 27017, "dev" );
 * Mongo mongo3 = new Mongo( new DBAddress( "127.0.0.1:27017", "dev" ) )
 * </pre></blockquote>
 */
public class Mongo extends DBTCP {

    /**
     *  Connects to the local mongo instance on default port.
     *
     *  @param dbname name of database to connect to
     *  @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo(String dbname) 
        throws UnknownHostException , MongoException {
        super(new DBAddress("127.0.0.1", dbname));
    }

    /**
     * Connects to Mongo using a given host, the default port (27017) and connects to a given database.
     * @param host the database's host address
     * @param dbName the name of the database to which to connect
     * @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host , String dbName )
        throws UnknownHostException , MongoException {
        super( new DBAddress( host , dbName ) );
    }

    /**
     * Connects to Mongo using a given host, port, and database.
     * @param host the database's host address
     * @param port the port on which the database is running
     * @param dbName the name of the database to which to connect
     * @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host , int port , String dbName )
        throws UnknownHostException , MongoException {
        super( new DBAddress( host , port , dbName ) );
    }

    /**
     * Connects to Mongo using a given DBAddress 
     * @see com.mongodb.DBAddress
     * @param addr the database address
     */
    public Mongo( DBAddress addr )
        throws MongoException {
        super( addr );
    }
}
