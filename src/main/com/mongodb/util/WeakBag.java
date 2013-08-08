// WeakBag.java

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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * if its not obvious what a weak bag should do, then, well...
 * very very not thread safe
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
public class WeakBag<T> implements Iterable<T> {

    /** Initializes a new weak bag. */
    public WeakBag(){
    }

    /** Adds an element to the bag.
     * @param t Element to add
     */
    public void add( T t ){
        _refs.add( new MyRef( t ) );
    }

    public boolean remove( T t ){

        for ( Iterator<MyRef> i = _refs.iterator(); i.hasNext(); ){
            MyRef ref = i.next();
            if( ref == null )
                continue;
            T me = ref.get();
            
            if ( me == null ){
                // this is just here cause i'm already doing the work, so why not
                i.remove();
                continue;
            }
            
            if ( me == t ){
                i.remove();
                return true;
            }                
        }
        return false;
    }

    public boolean contains( T t ){

        for ( Iterator<MyRef> i = _refs.iterator(); i.hasNext(); ){
            MyRef ref = i.next();
            T me = ref.get();
            if ( me == t )
                return true;
        }
        return false;
    }

    /** Returns the size of the bag.
     * @return the size of the bag
     */
    public int size(){
        clean();
        return _refs.size();
    }

    /** Removes all object from the bag. */
    public void clear(){
        _refs.clear();
    }
    
    /** Removes any null objects from the bag. */
    public void clean(){
        for ( Iterator<MyRef> i = _refs.iterator(); i.hasNext(); ){
            MyRef ref = i.next();
            if ( ref.get() == null )
                i.remove();
        }
    }

    public Iterator<T> iterator(){
        return getAll().iterator();
    }
    
    public List<T> getAll(){
    
        List<T> l = new ArrayList<T>();
        
        for ( Iterator<MyRef> i = _refs.iterator(); i.hasNext(); ){
            MyRef ref = i.next();
            T t = ref.get();
            if ( t == null )
                i.remove();
            else
                l.add( t );
        }        
        
        return l;
    }
    
    class MyRef extends WeakReference<T> {
        MyRef( T t ){
            super( t );
        }
    }

    private final List<MyRef> _refs = new LinkedList<MyRef>();
}
