// UniqueList.java

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

package com.mongodb.util;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class UniqueList<T> extends ArrayList<T> {

    private static final long serialVersionUID = -4415279469780082174L;

    public boolean add( T t ){
        if ( contains( t ) )
            return false;
        return super.add( t );
    }
    
    public boolean addAll(Collection<? extends T> c) {
        boolean added = false;
        for ( T t : c )
            added = added || add( t );
        return added;
    }

}
