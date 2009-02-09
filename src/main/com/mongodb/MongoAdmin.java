package com.mongodb;

import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;

/**
 *  Mongo adminstrative functions.
 * 
 */
public class MongoAdmin extends Mongo{

    protected static final String DB_NAME="admin";

    public MongoAdmin() throws UnknownHostException {
        this("127.0.0.1");
    }

    public MongoAdmin(String host) throws UnknownHostException {
        super(host, DB_NAME);
    }

    public MongoAdmin(String host, int port) throws UnknownHostException {
        super(host, port, DB_NAME);
    }

    public MongoAdmin(DBAddress addr) throws UnknownHostException {
        super(addr);
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

    
}
