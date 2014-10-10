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

package org.bson.types;

import org.bson.BSONObject;

/** 
 * A representation of the JavaScript Code with Scope BSON type.
 */
public class CodeWScope extends Code {

    private static final long serialVersionUID = -6284832275113680002L;

    /**
     * Construct an instance.
     *
     * @param code the code
     * @param scope the scope
     */
    public CodeWScope( String code , BSONObject scope ){
        super( code );
        _scope = scope;
    }

    /**
     * Gets the scope, which is is a mapping from identifiers to values, representing the scope in which the code should be evaluated.
     *
     * @return the scope
     */
    public BSONObject getScope(){
        return _scope;
    }

    @Override
    public boolean equals( Object o ){
        if ( ! ( o instanceof CodeWScope ) )
            return false;
        
        CodeWScope c = (CodeWScope)o;
        return _code.equals( c._code ) && _scope.equals( c._scope );
    }

    @Override
    public int hashCode(){
        return _code.hashCode() ^ _scope.hashCode();
    }

    final BSONObject _scope;
}

