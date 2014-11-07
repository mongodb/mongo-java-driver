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

package com.mongodb;

import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BSONByteBuffer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @deprecated This class will be removed in a future release.  There is no replacement.
 */
@Deprecated
public class LazyWriteableDBObject extends LazyDBObject {

    /**
     * @deprecated use {@link #LazyWriteableDBObject(byte[], org.bson.LazyBSONCallback)} instead
     */
    @Deprecated
    public LazyWriteableDBObject(BSONByteBuffer buff, LazyBSONCallback cbk){
        super(buff, cbk);
    }

    /**
     * @deprecated use {@link #LazyWriteableDBObject(byte[], int, org.bson.LazyBSONCallback)} instead
     */
    @Deprecated
    public LazyWriteableDBObject(BSONByteBuffer buff, int offset, LazyBSONCallback cbk){
        super(buff, offset, cbk);
    }


    public LazyWriteableDBObject(byte[] data, LazyBSONCallback cbk){
        this(data, 0, cbk);
    }

    public LazyWriteableDBObject(byte[] data, int offset, LazyBSONCallback cbk){
        super(data, offset, cbk);
    }

	@Override
	public Object put(String key, Object v) {
		return writeable.put(key, v);
	}

	@Override
	public void putAll(BSONObject o) {
		for(String key : o.keySet()){
			put(key, o.get(key));
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(Map m) {
		writeable.putAll(m);
	}

	@Override
	public Object get(String key) {
		Object o = writeable.get(key);
		return (o!=null) ? o : super.get(key);
	}

	@Override
	public Object removeField(String key) {
		Object o = writeable.remove(key);
		return (o!=null) ? o : super.removeField(key);
	}

	@Override
	public boolean containsField(String s) {
		boolean has = writeable.containsKey(s);
		return (has) ? has : super.containsField(s);
	}

	@Override
	public Set<String> keySet() {
		Set<String> combined = new HashSet<String>();
		combined.addAll(writeable.keySet());
		combined.addAll(super.keySet());
		return combined;
	}

	@Override
	public boolean isEmpty() {
		return writeable.isEmpty() || super.isEmpty();
	}

	final private HashMap<String, Object> writeable = new HashMap<String, Object>();
}
