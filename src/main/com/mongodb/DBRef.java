// DBRef.java

package com.mongodb;

public class DBRef {
    
    static final boolean D = Boolean.getBoolean( "DEBUG.DBREF" );

    /**
     *  CTOR used for testing BSON encoding.  Otherwise
     *  non-functional due to a DBRef needing a parent db object,
     *  a fieldName and a db
     *
     * @param ns namespace to point to
     * @param id value of _id
     */
    public DBRef(String ns, ObjectId id) {
        this (null, null, null, ns, id);
    }

    DBRef( DBObject parent , String fieldName , DBBase db , String ns , ObjectId id ){
    
        _parent = parent;
        _fieldName = fieldName;

        _db = db;
        
        _ns = ns;
        _id = id;
    }

    private DBObject fetch() {
        if (_loadedPointedTo)
            return _pointedTo;

        if (_db == null)
            throw new RuntimeException("no db");

        if (D) {
            System.out.println("following dbref.  parent.field:" + _fieldName + " ref to ns:" + _ns);
            Throwable t = new Throwable();
            t.fillInStackTrace();
            t.printStackTrace();
        }

        final DBCollection coll = _db.getCollectionFromString(_ns);

        _pointedTo = coll.find(_id);
        _loadedPointedTo = true;
        return _pointedTo;
    }

    final DBObject _parent;
    final String _fieldName;

    final ObjectId _id;
    final String _ns;
    final DBBase _db;

    private boolean _loadedPointedTo = false;
    private DBObject _pointedTo;
}
