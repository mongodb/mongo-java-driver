/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson;

import com.mongodb.LazyDBObject;
import org.bson.types.ObjectId;

import java.util.List;

/**
 * A {@code BSONCallback} for creation of {@code LazyBSONObject} and {@code LazyBSONList} instances.
 */
public class LazyBSONCallback extends EmptyBSONCallback {

    @Override
    public void reset(){
        _root = null;
    }

    @Override
    public Object get(){
        return _root;
    }

    @Override
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

    /**
     * Create a {@code LazyBSONObject} instance from the given bytes starting from the given offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the bytes
     * @return the LazyBSONObject
     */
    public Object createObject( byte[] bytes, int offset ){
        return new LazyDBObject( bytes, offset, this );
    }

    /**
     * Create a {@code LazyBSONList} from the given bytes starting from the given offset.
     *
     * @param bytes the raw BSON bytes
     * @param offset the offset into the bytes
     * @return the LazyBSONList
     */
    @SuppressWarnings("rawtypes")
	public List createArray( byte[] bytes, int offset ){
        return new LazyBSONList( bytes, offset, this );
    }

    /**
     * This is a factory method pattern to create appropriate objects for BSON type DBPointer(0x0c).
     *
     * @param ns the namespace of the reference
     * @param id the identifier of the reference
     * @return object to be used as reference representation
     */
    public Object createDBRef( String ns, ObjectId id ){
        return new BasicBSONObject( "$ns", ns ).append( "$id", id );
    }

    private Object _root;
}
