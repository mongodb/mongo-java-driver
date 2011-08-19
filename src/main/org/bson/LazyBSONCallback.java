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

import org.bson.types.*;

import java.util.logging.*;

/**
 *
 */
public class LazyBSONCallback implements BSONCallback {

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

    public BSONCallback createBSONCallback(){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void arrayStart(){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void arrayStart( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public Object arrayDone(){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotNull( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotUndefined( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotMinKey( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotMaxKey( String name ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotBoolean( String name, boolean v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotDouble( String name, double v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotInt( String name, int v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotLong( String name, long v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotDate( String name, long millis ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotString( String name, String v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotSymbol( String name, String v ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotRegex( String name, String pattern, String flags ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotTimestamp( String name, int time, int inc ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotObjectId( String name, ObjectId id ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotDBRef( String name, String ns, ObjectId id ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    @Deprecated
    public void gotBinaryArray( String name, byte[] data ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotBinary( String name, byte type, byte[] data ){
        setRootObject( createObject( data, 0 ) );
    }

    public void gotUUID( String name, long part1, long part2 ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotCode( String name, String code ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void gotCodeWScope( String name, String code, Object scope ){
        throw new UnsupportedOperationException( "Not supported yet." );
    }

    public void setRootObject( Object root ){
        _root = root;
    }

    public Object createObject( byte[] data, int offset ){
        // TODO - LazyBSONList ?
        return new LazyBSONObject( data, offset, this );
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
