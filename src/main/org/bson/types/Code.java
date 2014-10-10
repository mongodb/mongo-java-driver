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

// Code.java

package org.bson.types;

import java.io.Serializable;

/** 
 * For using the Code type.
 */
public class Code implements Serializable {

    private static final long serialVersionUID = 475535263314046697L;

    /**
     * Construct a new instance with the given code.
     *
     * @param code the Javascript code
     */
    public Code( final String code ){
        _code = code;
    }

    /**
     * Get the Javascript code.
     *
     * @return the code
     */
    public String getCode(){
        return _code;
    }

    @Override
    public boolean equals(final Object o) {
        if ( ! ( o instanceof Code ) )
            return false;
        
        Code c = (Code)o;
        return _code.equals( c._code );
    }

    @Override
    public int hashCode(){
        return _code.hashCode();
    }

    @Override
    public String toString() {
        return getCode();
    }

    final String _code;

}

