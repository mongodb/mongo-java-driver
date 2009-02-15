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

package com.mongodb.util.gridfs;

import com.mongodb.ObjectId;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *  Binary object managed by the GridFS system
 * 
 */
public class GridFSObject {

    protected ObjectId _id;
    protected String _filename;
    protected String _contentType = GridFS.DEFAULT_MIMETYPE;
    protected long _length = 0;
    protected int _chunkSize = GridFS.DEFAULT_CHUNKSIZE;
    protected Date _uploadDate = new Date();
    protected List<String> _aliases = new ArrayList<String>();
    protected BasicDBObject _metadata = new BasicDBObject();
    protected byte[] _myBuffer = new byte[GridFS.DEFAULT_CHUNKSIZE];
    protected InputStream _inStream;
    protected int _nextChunkID = 0;
    protected GridFS _gridfs;

    protected DBCursor _chunkCursor;
    /**
     *  Creates a new object out of a stream.
     *
     * @param name Name for this object
     * @param inStream binary stream of data for this object
     */
    public GridFSObject(String name, InputStream inStream) {

        _id = new ObjectId();
        _filename = name;
        _inStream  = inStream;
    }

    protected GridFSObject(GridFS gridfs,  DBObject o) {
        _gridfs = gridfs;

        _id = (ObjectId) o.get("_id");
        _filename = (String) o.get("filename");
        _contentType = (String) o.get("contentType");

        Double d = (Double) o.get("length");

        _length = d.longValue();
        _chunkSize = (Integer) o.get("chunkSize");
        _uploadDate = (Date) o.get("uploadDate");

//        TODO  _metadata, _aliases


        _chunkCursor = gridfs.getChunkCursorForFile(_id);

    }

    public InputStream getInputStream() {
        return new GridFSInputStream(this);
    }

    protected GridFSChunk getNextChunkFromDB() {

        if (!_chunkCursor.hasNext())  {
            return null;
        }

        return new GridFSChunk(_chunkCursor.next());
    }

    protected GridFSChunk getNextChunkFromStream() throws IOException {
        int len = _inStream.read(_myBuffer);
        return (len <= 0 ? null : new GridFSChunk(_id, _myBuffer, len, _nextChunkID++));
    }
    
    public String getFilename() {
        return _filename;
    }

    public void setFilename(String filename) {
        _filename = filename;
    }

    public String getContentType() {
        return _contentType;
    }

    public void setContentType(String contentType) {
        _contentType = contentType;
    }

    protected void setLength(long l) {
        _length = l;
    }

    public ObjectId getID() {
        return _id;
    }

    /**
     * Returns this chunk as a DBObject suitable for DB insertion
     * @return this as DB Object
     */
    protected DBObject getDBObject() {

        BasicDBObject o = new BasicDBObject();

        o.put("_id", _id);
        o.put("filename", _filename);
        o.put("contentType", _contentType);
        o.put("length", _length);
        o.put("chunkSize", _chunkSize);
        o.put("uploadDate", _uploadDate);
        o.put("aliases", _aliases);
        o.put("metadata", _metadata);
        
        return o;
    }
}
