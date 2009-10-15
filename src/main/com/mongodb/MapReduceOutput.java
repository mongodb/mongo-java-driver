// MapReduceOutput.java

package com.mongodb;

public class MapReduceOutput {

    MapReduceOutput( DBCollection from , BasicDBObject raw ){
        _collname = raw.getString( "result" );
        _coll = from._base.getCollection( _collname );
        _counts = (BasicDBObject)raw.get( "counts" );
    }

    public DBCursor results(){
        return _coll.find();
    }
    
    final String _collname;
    final DBCollection _coll;
    final BasicDBObject _counts;
}
