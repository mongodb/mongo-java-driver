// BSONTimestamp.java

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

package org.bson.types;

import java.util.Date;

/**
   this is used for internal increment values.
   for storing normal dates in MongoDB, you should use java.util.Date
 */
public class BSONTimestamp {
    
    static final boolean D = Boolean.getBoolean( "DEBUG.DBTIMESTAMP" );
    
    public BSONTimestamp(){
        _inc = 0;
        _time = null;
    }

    public BSONTimestamp(int time, int i) {
        _time = new Date( time * 1000L );
        _inc = i;
    }
    
    public int getTime(){
        if ( _time == null )
            return 0;
        return (int)(_time.getTime() / 1000);
    }
    
    public int getInc(){
        return _inc;
    }

    public String toString(){
        return "TS time:" + _time + " inc:" + _inc;
    }
    
    final int _inc;
    final Date _time;
}
