package com.mongodb;

import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;

/**
 *  Mongo adminstrative functions.
 * 
 */
public class MongoAdmin extends Mongo { 

    protected static final String DB_NAME="admin";

    protected final DBAddress _usersDBAddress;

    public MongoAdmin() throws UnknownHostException {
        this("127.0.0.1");
    }

    public MongoAdmin(String host) throws UnknownHostException {
        this(host, 27017);
    }

    public MongoAdmin(String host, int port) throws UnknownHostException {
        this(new DBAddress(host, port, DB_NAME));
    }

    public MongoAdmin(DBAddress addr) throws UnknownHostException {
        super(addr);

        _usersDBAddress = addr;
    }

    /**
     *   Returns a list of of the names of the databases available on the current server
     *
     *   @return list of database names
     */
    public List<String> getDatabaseNames() {
        BasicDBObject cmd = new BasicDBObject();
        cmd.put("listDatabases", 1);


        BasicDBObject res = (BasicDBObject) command(cmd);

        if (res.getInt("ok" , 0 ) != 1 ){
            throw new RuntimeException( "error counting : " + res );
        }

        BasicDBList l = (BasicDBList) res.get("databases");

        List<String> list = new ArrayList<String>();

        for (Object o : l) {
            list.add(((BasicDBObject) o).getString("name"));
        }
        return list;
    }

    /**
     *   Returns an active Mongo object for the specified DB
     *   @param dbName name of database to get a connection to
     *   @return mongo database object
     */
    public Mongo getDatabase(String dbName) {

        try {
            DBAddress addr = new DBAddress(_usersDBAddress._host, _usersDBAddress._port, dbName);
            return new Mongo(addr);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Error : address is no longer valid", e);
        }
    }

    /**
     *  Drops the database if it exists.
     *
     * @param dbName name of database to drop
     */
    public void dropDatabase(String dbName){

        Mongo m = getDatabase(dbName);
        m.dropDatabase();
    }
}