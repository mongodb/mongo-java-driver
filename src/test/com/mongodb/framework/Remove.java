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

package com.mongodb.framework;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;


public class Remove {

    public static void main(String[] args) 
        throws Exception {
        
        DB db = new MongoClient().getDB( "driver_test_framework" );
        DBObject foo = new BasicDBObject();
        db.getCollection( "remove1" ).remove( foo );

        foo.put( "a", 3 );
        db.getCollection( "remove2" ).remove( foo );
    }
}
