/**
 *      Copyright (C) 2011 10gen Inc.
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

import java.util.HashMap;

import org.bson.io.BSONByteBuffer;

/**
 * @author brendan
 * @author scotthernandez
 *
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class KeyCachingLazyBSONObject extends LazyBSONObject {

    public KeyCachingLazyBSONObject(byte[] data , LazyBSONCallback cbk) { super( data , cbk ); }
    public KeyCachingLazyBSONObject(byte[] data , int offset , LazyBSONCallback cbk) { super( data , offset , cbk ); }
    public KeyCachingLazyBSONObject( BSONByteBuffer buffer, LazyBSONCallback callback ){ super( buffer, callback ); }
    public KeyCachingLazyBSONObject( BSONByteBuffer buffer, int offset, LazyBSONCallback callback ){ super( buffer, offset, callback ); }

    @Override
    public Object get( String key ) {
        ensureFieldList();
        return super.get( key );
    }

    @Override
    public boolean containsField( String s ) {
        ensureFieldList();
        if (! fieldIndex.containsKey( s ) )
            return false;
        else 
            return super.containsField( s );
    }
    
    synchronized private void ensureFieldList() {
        //only run once
        if (fieldIndex == null) return;
        try {
            int offset = _doc_start_offset + FIRST_ELMT_OFFSET;
            
            while ( !isElementEmpty( offset ) ){
                int fieldSize = sizeCString( offset );
                int elementSize = getElementBSONSize( offset++ );
                String name = _input.getCString( offset );
                ElementRecord _t_record = new ElementRecord( name, offset );
                fieldIndex.put( name, _t_record );
                offset += ( fieldSize + elementSize );
            }
        } catch (Exception e) {
            fieldIndex = new HashMap<String, ElementRecord>();
        }
    }
    
    
    private HashMap<String, ElementRecord> fieldIndex = new HashMap<String, ElementRecord>();

}
