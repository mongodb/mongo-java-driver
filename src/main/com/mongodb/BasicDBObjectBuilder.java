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

public class BasicDBObjectBuilder {
    
    public static BasicDBObjectBuilder start(){
        return new BasicDBObjectBuilder();
    }

    public BasicDBObjectBuilder add( String key , Object val ){
        _it.put( key , val );
        return this;
    }
    
    public DBObject get(){
        return _it;
    }

    DBObject _it = new BasicDBObject();

}
