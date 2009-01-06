// WeakBag.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package ed.util;

import java.lang.ref.*;
import java.util.*;

/**
 * if its not obvious what a weak bag should do, then, well...
 * very very not thead safe
 * @expose
 */
public class WeakBag<T> implements Iterable<T> {

    /** Initializes a new weak bag. */
    public WeakBag(){
    }

    /** Adds an element to the bag.
     * @param t Element to add
     */
    public void add( T t ){
        _set.add( new MyRef( t ) );
    }

    public boolean remove( T t ){

        for ( Iterator<MyRef> i = _set.iterator(); i.hasNext(); ){
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
        
        for ( Iterator<MyRef> i = _set.iterator(); i.hasNext(); ){
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
        return _set.size();
    }

    /** Removes all object from the bag. */
    public void clear(){
        _set.clear();
    }
    
    /** Removes any null objects from the bag. */
    public void clean(){
        for ( Iterator<MyRef> i = _set.iterator(); i.hasNext(); ){
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
        
        for ( Iterator<MyRef> i = _set.iterator(); i.hasNext(); ){
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

    /** @unexpose */
    private final List<MyRef> _set = new ArrayList<MyRef>();
}
