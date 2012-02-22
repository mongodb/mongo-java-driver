// Code.java

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

import java.io.Serializable;
import java.util.*;

import org.bson.*;

/** 
 * for using the Code type
 */
public class Code implements Serializable {

    private static final long serialVersionUID = 475535263314046697L;

    public Code( String code ){
        _code = code;
    }

    public String getCode(){
        return _code;
    }

    public boolean equals( Object o ){
        if ( ! ( o instanceof Code ) )
            return false;
        
        Code c = (Code)o;
        return _code.equals( c._code );
    }

    public int hashCode(){
        return _code.hashCode();
    }

    @Override
    public String toString() {
        return getCode();
    }

    final String _code;

}

