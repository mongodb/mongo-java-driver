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

import java.net.*;
import java.util.*;

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
 *
 * Mongo instances have connection pooling built in - see the requestStart
 * and requestDone methods for more information.
 */
public class Mongo {

    public Mongo()
        throws UnknownHostException , MongoException {
        this( new DBAddress( "test" ) );
    }

    /**
     *  Connects to the local mongo instance on default port.
     *
     *  @param dbname name of database to connect to
     *  @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host )
        throws UnknownHostException , MongoException {
        this( new DBAddress( host , "test" ) );
    }

    /**
     * Connects to Mongo using a given host, port, and database.
     * @param host the database's host address
     * @param port the port on which the database is running
     * @param dbName the name of the database to which to connect
     * @throws UnknownHostException if the database host cannot be resolved
     */
    public Mongo( String host , int port )
        throws UnknownHostException , MongoException {
        this( new DBAddress( host , port , "test" ) );
    }

    /**
     * Connects to Mongo using a given DBAddress
     * @see com.mongodb.DBAddress
     * @param addr the database address
     */
    public Mongo( DBAddress addr )
        throws MongoException {
        _addr = addr;
        _addrs = null;
    }

    /**
       creates a Mongo connection in paired mode
       * @param left left side of the pair
       * @param right right side of the pair
     */
    public Mongo( DBAddress left , DBAddress right )
        throws MongoException {
        _addr = null;
        _addrs = Arrays.asList( left , right );
    }

    public DB getDB( String db ){
        if ( _addr != null )
            return new DBTCP( _addr.getSister( db ) );
        
        List<DBAddress> l = new ArrayList<DBAddress>();
        for ( DBAddress a : _addrs )
            l.add( a.getSister( db ) );
        return new DBTCP( l );
    }
    
    public List<String> getDatabaseNames()
        throws MongoException {

        BasicDBObject cmd = new BasicDBObject();
        cmd.put("listDatabases", 1);
        

        BasicDBObject res = (BasicDBObject)getDB( "admin" ).command(cmd);

        if (res.getInt("ok" , 0 ) != 1 )
            throw new MongoException( "error listing databases : " + res );

        BasicDBList l = (BasicDBList)res.get("databases");

        List<String> list = new ArrayList<String>();

        for (Object o : l) {
            list.add(((BasicDBObject)o).getString("name"));
        }
        return list;
    }


    /**
     *  Drops the database if it exists.
     *
     * @param dbName name of database to drop
     */
    public void dropDatabase(String dbName)
        throws MongoException {
        
        getDB( dbName ).dropDatabase();
    }

    final DBAddress _addr;
    final List<DBAddress> _addrs;
}
