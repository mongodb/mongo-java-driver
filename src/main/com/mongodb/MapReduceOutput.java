// MapReduceOutput.java

package com.mongodb;

/**
 * Represents the result of a map/reduce operation
 * @author antoine
 */
public class MapReduceOutput {

    MapReduceOutput( DBCollection from , BasicDBObject raw ){
        _collname = raw.getString( "result" );
        _coll = from._db.getCollection( _collname );
        _counts = (BasicDBObject)raw.get( "counts" );
    }

    /**
     * returns a cursor to the results of the operation
     * @return
     */
    public DBCursor results(){
        return _coll.find();
    }

    /**
     * drops the collection that holds the results
     */
    public void drop(){
        _coll.drop();
    }
    
    /**
     * gets the collection that holds the results
     * @return
     */
    public DBCollection getOutputCollection(){
        return _coll;
    }

    final String _collname;
    final DBCollection _coll;
    final BasicDBObject _counts;
}
