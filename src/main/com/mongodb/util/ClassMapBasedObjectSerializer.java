
/**
 *      Copyright (C) 2012 10gen Inc.
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

package com.mongodb.util;

import com.mongodb.Bytes;
import org.bson.util.ClassMap;

import java.util.List;

/**
 * Objects of type ClassMapBasedObjectSerializer are constructed to perform
 * instance specific object to JSON serialization schemes.
 * <p>
 * This class is not thread safe
 *
 * @author breinero
 */
class ClassMapBasedObjectSerializer extends AbstractObjectSerializer {

    /**
     * Assign a ObjectSerializer to perform a type specific serialization scheme
     * @param c this object's type serves as a key in the serialization map.
     * ClassMapBasedObjectSerializer uses org.bson.util.ClassMap and not only checks if 'c' is a key in the Map,
     * but also walks the up superclass and interface graph of 'c' to find matches.
     * This means that it is only necessary assign ObjectSerializers to base classes. @see org.bson.util.ClassMap
     * @param serializer performs the serialization mapping specific to the @param key type
     */
    void addObjectSerializer(Class c, ObjectSerializer serializer) {
        _serializers.put(c , serializer);
    }
    
    /**
     * 
     * @param obj the object to be serialized
     * @param buf StringBuilder containing the JSON representation of the object
     */
    @Override
    public void serialize(Object obj, StringBuilder buf){
            
        obj = Bytes.applyEncodingHooks( obj );
        
        if(obj == null) {
            buf.append(" null ");
            return;
        }
        
        ObjectSerializer serializer = null;
        
        List<Class<?>> ancestors;
        ancestors = ClassMap.getAncestry(obj.getClass());

        for (final Class<?> ancestor : ancestors) {
            serializer = _serializers.get(ancestor);
            if (serializer != null)
                break;
        }
        
        if (serializer == null && obj.getClass().isArray())
            serializer = _serializers.get(Object[].class);
        
        if (serializer == null)
            throw new RuntimeException( "json can't serialize type : " + obj.getClass() );
        
        serializer.serialize(obj, buf);
    }
    
    private ClassMap<ObjectSerializer> _serializers = new ClassMap<ObjectSerializer>();
}
