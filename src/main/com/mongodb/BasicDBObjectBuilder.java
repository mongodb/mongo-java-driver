// BasicDBObjectBuilder.java

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

package com.mongodb;

import java.util.Map;
import java.util.Iterator;


/**
 * utility for building objects
 * example:
 *  BasicDBObjectBuilder.start().add( "name" , "eliot" ).add( "number" , 17 ).get()
 */
public class BasicDBObjectBuilder {
    
    public static BasicDBObjectBuilder start(){
        return new BasicDBObjectBuilder();
    }

    public static BasicDBObjectBuilder start( String k , Object val ){
        return (new BasicDBObjectBuilder()).add( k , val );
    }

    /**
     * Creates an object builder from an existing map.
     * @param m map to use
     * @return the new builder
     */
    public static BasicDBObjectBuilder start(Map m){
        BasicDBObjectBuilder b = new BasicDBObjectBuilder();
        Iterator<Map.Entry> i = m.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry entry = i.next();
            b.add(entry.getKey().toString(), entry.getValue());
        }
        return b;
    }

    /**
     * @return returns itself so you can chain .append( "a" , 1 ).add( "b" , 1 )
     */
    public BasicDBObjectBuilder append( String key , Object val ){
        _it.put( key , val );
        return this;
    }


    /**
     * @return returns itself so you can chain  .add( "a" , 1 ).add( "b" , 1 )
     */
    public BasicDBObjectBuilder add( String key , Object val ){
        _it.put( key , val );
        return this;
    }
    
    public DBObject get(){
        return _it;
    }

    DBObject _it = new BasicDBObject();

}
