// GridFSFile.java

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

package com.mongodb.gridfs;

import com.mongodb.*;
import com.mongodb.util.*;

import java.io.*;
import java.util.*;

public abstract class GridFSFile implements DBObject {

    
    // ------------------------------
    // --------- db           -------
    // ------------------------------

    public void save(){
        if ( _fs == null )
            throw new MongoException( "need _fs" );
        _fs._filesCollection.save( this );
    }

    public void validate(){
        if ( _fs == null )
            throw new MongoException( "no _fs" );
        if ( _md5 == null )
            throw new MongoException( "no _md5 stored" );
        
        DBObject res = _fs._db.command( new BasicDBObject( "filemd5" , _id ) );
        String m = res.get( "md5" ).toString();
        if ( m.equals( _md5 ) )
            return;

        throw new MongoException( "md5 differ.  mine [" + _md5 + "] theirs [" + m + "]" );
    }

    public int numChunks(){
        double d = _length;
        d = d / _chunkSize;
        return (int)Math.ceil( d );
    }

    // ------------------------------
    // --------- getters      -------
    // ------------------------------


    public Object getId(){
        return _id;
    }

    public String getFilename(){
        return _filename;
    }

    public String getContentType(){
        return _contentType;
    }

    public long getLength(){
        return _length;
    }
    
    public long getChunkSize(){
        return _chunkSize;
    }
    
    public Date getUploadDate(){
        return _uploadDate;
    }

    public List<String> getAliases(){
        return _aliases;
    }

    public DBObject getMetaData(){
        return _metadata;
    }
    
    public String getMD5(){
        return _md5;
    }

    // ------------------------------
    // --------- DBOBject methods ---
    // ------------------------------
    
    public Object put( String key , Object v ){
        if ( key == null )
            throw new RuntimeException( "key should never be null" );
        else if ( key.equals( "_id" ) )
            _id = v;
        else if ( key.equals( "_ns"  ) );
        else if ( key.equals( "filename" ) )
            _filename = v == null ? null : v.toString();
        else if ( key.equals( "contentType" ) )
            _contentType = (String)v;
        else if ( key.equals( "length" ) )
            _length = ((Number)v).longValue();
        else if ( key.equals( "chunkSize" ) )
            _chunkSize = ((Number)v).longValue();
        else if ( key.equals( "uploadDate" ) )
            _uploadDate = (Date)v;
        else if ( key.equals( "metadata" ) )
            _metadata = (DBObject)v;
        else if ( key.equals( "md5" ) )
            _md5 = (String)v;
        else if ( key.equals( "aliases" ) ){
            if ( v != null ){
                throw new MongoException( "can't handle aliases yet" );
            }
        }
        else 
            throw new MongoException( "GridFSFile don't know about key [" + key + "] converting to metadata " );
        return v;
    }

    public Object get( String key ){
        if ( key == null )
            throw new RuntimeException( "key should never be null" );
        else if ( key.equals( "_id" ) )
            return _id;
        else if ( key.equals( "filename" ) )
            return _filename;
        else if ( key.equals( "contentType" ) )
            return _contentType;
        else if ( key.equals( "length" ) )
            return _length;
        else if ( key.equals( "chunkSize" ) )
            return _chunkSize;
        else if ( key.equals( "uploadDate" ) )
            return _uploadDate;
        else if ( key.equals( "metadata" ) )
            return _metadata;
        else if ( key.equals( "md5" ) )
            return _md5;
        

        return null;
    }


    public void putAll( DBObject o ){
        throw new UnsupportedOperationException();
    }
    public void putAll( Map m ){
        throw new UnsupportedOperationException();
    }
    public Map toMap(){
        throw new UnsupportedOperationException();
    }
    public Object removeField( String key ){
        throw new UnsupportedOperationException();
    }

    public boolean containsKey( String s ){
        return containsField( s );
    }
    public boolean containsField(String s){
        return VALID_FIELDS.contains( s );
    }

    public Set<String> keySet(){
        return VALID_FIELDS;
    }

    public boolean isPartialObject(){
        return false;
    }
    public void markAsPartialObject(){
        throw new RuntimeException( "can't load partial GridFSFile file" );
    }
    
    // ----------------------
    // ------- fields -------
    // ----------------------

    public String toString(){
        return JSON.serialize( this );
    }

    protected void setGridFS( GridFS fs ){
        _fs = fs;
    }

    protected GridFS _fs = null;

    Object _id;
    String _filename;
    String _contentType;
    long _length;
    long _chunkSize;
    Date _uploadDate;
    List<String> _aliases;
    DBObject _metadata;
    String _md5;

    final static Set<String> VALID_FIELDS = Collections.unmodifiableSet( new HashSet( Arrays.asList( new String[]{ 
                    "_id" , "filename" , "contentType" , "length" , "chunkSize" , 
                    "uploadDate" , "aliases" , "metadata" , "md5" 
                } ) ) );
}
