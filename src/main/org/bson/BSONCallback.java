// BSONCallback.java

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

import org.bson.types.ObjectId;

public interface BSONCallback {
    
    void objectStart();
    void objectStart(String name);
    void objectStart(boolean array);
    Object objectDone();

    void reset();
    Object get();
    BSONCallback createBSONCallback();

    void arrayStart();
    void arrayStart(String name);
    Object arrayDone();
    
    void gotNull( String name );
    void gotUndefined( String name );
    void gotMinKey( String name );
    void gotMaxKey( String name );
    
    void gotBoolean( String name , boolean v );
    void gotDouble( String name , double v );
    void gotInt( String name , int v );
    void gotLong( String name , long v );
    
    void gotDate( String name , long millis );
    void gotString( String name , String v );
    void gotSymbol( String name , String v );
    void gotRegex( String name , String pattern , String flags );

    void gotTimestamp( String name , int time , int inc );
    void gotObjectId( String name , ObjectId id );
    void gotDBRef( String name , String ns , ObjectId id );
    
    /**
     * 
     */
    @Deprecated
    void gotBinaryArray( String name , byte[] data );
    void gotBinary( String name , byte type , byte[] data );
    /**
     * subtype 3
     */
    void gotUUID( String name , long part1, long part2);

    void gotCode( String name , String code );
    void gotCodeWScope( String name , String code , Object scope );
}
