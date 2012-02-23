// CodeWScope.java

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

import org.bson.*;

/** 
 * for using the CodeWScope type
 */
public class CodeWScope extends Code {

    private static final long serialVersionUID = -6284832275113680002L;

    public CodeWScope( String code , BSONObject scope ){
        super( code );
        _scope = scope;
    }

    public BSONObject getScope(){
        return _scope;
    }

    public boolean equals( Object o ){
        if ( ! ( o instanceof CodeWScope ) )
            return false;
        
        CodeWScope c = (CodeWScope)o;
        return _code.equals( c._code ) && _scope.equals( c._scope );
    }

    public int hashCode(){
        return _code.hashCode() ^ _scope.hashCode();
    }

    final BSONObject _scope;
}

