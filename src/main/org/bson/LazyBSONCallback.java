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
package org.bson;

import java.util.List;
import java.util.logging.Logger;

import com.mongodb.LazyDBObject;
import org.bson.types.ObjectId;

/**
 *
 */
public class LazyBSONCallback extends EmptyBSONCallback {

    public void objectStart(){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void objectStart( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void objectStart( boolean array ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public Object objectDone(){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void reset(){
        _root = null;
    }

    public Object get(){
        return _root;
    }

    public void gotBinary( String name, byte type, byte[] data ){
        setRootObject( createObject( data, 0 ) );
    }

    /**
     * @deprecated This method is NOT a part of public API and will be dropped in 3.x versions.
     */
    @Deprecated
    public void setRootObject( Object root ){
        _root = root;
    }

    public Object createObject( byte[] data, int offset ){
        return new LazyDBObject( data, offset, this );
    }

    @SuppressWarnings("rawtypes")
	public List createArray( byte[] data, int offset ){
        return new LazyBSONList( data, offset, this );
    }

    public Object createDBRef( String ns, ObjectId id ){
        return new BasicBSONObject( "$ns", ns ).append( "$id", id );
    }


    /*    public Object createObject(InputStream input, int offset) {
        try {
            return new LazyBSONObject(input, offset, this);
        }
        catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }*/
    private Object _root;
    private static final Logger log = Logger.getLogger( "org.bson.LazyBSONCallback" );
}
