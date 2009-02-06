package com.mongodb.util.gridfs;

import com.mongodb.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 *  Represents a chunk of data in a GridFS object.
 * 
 */
class GridFSChunk {

    protected ObjectId _id;
    protected ObjectId _files_id;
    protected int _n;
    protected byte[] _data;

    protected int _size;

    /**
     *  Creates from a byte buffer
     *
     * @param fileId  parent file id
     * @param data array of data for this chunk
     * @param len  length of data in the buffer
     * @param n chunk number for this file
     */
    GridFSChunk(ObjectId fileId, byte[] data, int len, int n) {
        _id = new ObjectId();
        _files_id = fileId;
        _n = n;
        _data = new byte[len];
        System.arraycopy(data, 0, _data, 0, len);
        _size = len;
    }

    /**
     *  Hydrates this from a DBObject - used when reading chunks
     *  from the database
     *
     * @param o object to get data from
     */
    GridFSChunk(DBObject o) {
        _id = (ObjectId) o.get("_id");
        _files_id = (ObjectId) o.get("files_id");
        _n = (Integer) o.get("n");
        _data = (byte[]) o.get("data");
        _size = _data.length;
    }

    /**
     *  Returns the size of this chunk in bytes
     * @return size of chunk
     */
    protected long getSize() {
        return _size;
    }
    
    /**
     * Returns this chunk as a DBObject suitable for DB insertion
     * @return this as DB Object
     */
    protected DBObject getDBObject() {

        BasicDBObject o = new BasicDBObject();

        o.put("_id", _id);
        o.put("files_id", _files_id);
        o.put("n", _n);
        o.put("data", _data);

        return o;
    }
}
