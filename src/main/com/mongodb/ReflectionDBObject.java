// ReflectionDBObject.java

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

import java.util.*;
import java.lang.reflect.*;

import com.mongodb.util.*;

public abstract class ReflectionDBObject implements DBObject {
    
    public Object get( String key ){
        return getWrapper().get( this , key );
    }

    public Set<String> keySet(){
        return getWrapper().keySet();
    }

    /**
     * @deprecated
     */
    public boolean containsKey( String s ){
        return containsField( s );
    }

    public boolean containsField( String s ){
        return getWrapper().containsKey( s );
    }

    public Object put( String key , Object v ){
        return getWrapper().set( this , key , v );
    }

    public void putAll( Map m ){
        for ( Object k : m.keySet() ){
            put( k.toString() , m.get( k ) );
        }
    } 
    
    public void putAll( DBObject o ){
        for ( String k : o.keySet() ){
            put( k , o.get( k ) );
        }
    }

    public ObjectId get_id(){
        return _id;
    }

    public void set_id( ObjectId id ){
        _id = id;
    }

    public boolean isPartialObject(){
        return false;
    }

    public Map toMap() {
       Map m = new HashMap();
       Iterator i = this.keySet().iterator();
       while (i.hasNext()) {
           Object s = i.next();
           m.put(s, this.get(s+""));
       }
       return m;
    }

    public void markAsPartialObject(){
        throw new RuntimeException( "ReflectionDBObjects can't be partial" );
    }

    public Object removeField( String key ){
        throw new RuntimeException( "can't remove from a ReflectionDBObject" );
    }

    JavaWrapper getWrapper(){
        if ( _wrapper != null )
            return _wrapper;

        _wrapper = getWrapper( this.getClass() );
        return _wrapper;
    }

    JavaWrapper _wrapper;
    ObjectId _id;

    public static class JavaWrapper {
        JavaWrapper( Class c ){
            _class = c;
            _name = c.getName();

            _fields = new TreeMap<String,FieldInfo>();
            for ( Method m : c.getMethods() ){
                if ( ! ( m.getName().startsWith( "get" ) || m.getName().startsWith( "set" ) ) )
                    continue;
                
                String name = m.getName().substring(3);
                if ( name.length() == 0 || IGNORE_FIELDS.contains( name ) )
                    continue;

                Class type = m.getName().startsWith( "get" ) ? m.getReturnType() : m.getParameterTypes()[0];

                FieldInfo fi = _fields.get( name );
                if ( fi == null ){
                    fi = new FieldInfo( name , type );
                    _fields.put( name , fi );
                }
                
                if ( m.getName().startsWith( "get" ) )
                    fi._getter = m;
                else
                    fi._setter = m;
            }

            Set<String> names = new HashSet<String>( _fields.keySet() );
            for ( String name : names )
                if ( ! _fields.get( name ).ok() )
                    _fields.remove( name );
            
            _keys = Collections.unmodifiableSet( _fields.keySet() );
        }

        public Set<String> keySet(){
            return _keys;
        }

        public boolean containsKey( String key ){
            return _keys.contains( key );
        }

        public Object get( ReflectionDBObject t , String name ){
            FieldInfo i = _fields.get( name );
            if ( i == null )
                return null;
            try {
                return i._getter.invoke( t );
            }
            catch ( Exception e ){
                throw new RuntimeException( "could not invoke getter for [" + name + "] on [" + _name + "]" , e );
            }
        }

        public Object set( ReflectionDBObject t , String name , Object val ){
            if ( IGNORE_SETS.contains( name ) )
                return null;
            FieldInfo i = _fields.get( name );
            if ( i == null )
                throw new IllegalArgumentException( "no field [" + name + "] on [" + _name + "]" );
            try {
                return i._setter.invoke( t , val );
            }
            catch ( Exception e ){
                throw new RuntimeException( "could not invoke setter for [" + name + "] on [" + _name + "]" , e );
            }
        }

        Class getInternalClass( String path ){
            String cur = path;
            String next = null;
            final int idx = path.indexOf( "." );
            if ( idx >= 0 ){
                cur = path.substring( 0 , idx );
                next = path.substring( idx + 1 );
            }
            
            FieldInfo fi = _fields.get( cur );
            if ( fi == null )
                return null;
            
            if ( next == null )
                return fi._class;
            
            JavaWrapper w = getWrapperIfReflectionObject( fi._class );
            if ( w == null )
                return null;
            return w.getInternalClass( next );
        }
        
        final Class _class;
        final String _name;
        final Map<String,FieldInfo> _fields;
        final Set<String> _keys;
    }
    
    static class FieldInfo {
        FieldInfo( String name , Class c ){
            _name = name;
            _class = c;
        }

        boolean ok(){
            return 
                _getter != null &&
                _setter != null;
        }
        
        final String _name;
        final Class _class;
        Method _getter;
        Method _setter;
    }
        
    public static JavaWrapper getWrapperIfReflectionObject( Class c ){
        if ( ReflectionDBObject.class.isAssignableFrom( c ) )
            return getWrapper( c );
        return null;
    }

    public static JavaWrapper getWrapper( Class c ){
        JavaWrapper w = _wrappers.get( c );
        if ( w == null ){
            w = new JavaWrapper( c );
            _wrappers.put( c , w );
        }
        return w;
    }
    
    private static final Map<Class,JavaWrapper> _wrappers = Collections.synchronizedMap( new HashMap<Class,JavaWrapper>() );
    private static final Set<String> IGNORE_FIELDS = new HashSet<String>();
    static {
        IGNORE_FIELDS.add( "Int" );
    }

    private static final Set<String> IGNORE_SETS = new HashSet<String>();
    static {
        IGNORE_SETS.add( "_save" );
        IGNORE_SETS.add( "_update" );
        IGNORE_SETS.add( "_ns" );
    }

    
}
