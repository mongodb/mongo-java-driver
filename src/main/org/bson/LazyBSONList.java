package org.bson;

import org.bson.io.BSONByteBuffer;

import java.util.*;

@SuppressWarnings( "rawtypes" )
public class LazyBSONList extends LazyBSONObject implements List {

    public LazyBSONList(byte[] data , LazyBSONCallback callback) { super( data , callback ); }
    public LazyBSONList(byte[] data , int offset , LazyBSONCallback callback) { super( data , offset , callback ); }
    public LazyBSONList(BSONByteBuffer buffer , LazyBSONCallback callback) { super( buffer , callback ); }
    public LazyBSONList(BSONByteBuffer buffer , int offset , LazyBSONCallback callback) { super( buffer , offset , callback ); }

    @Override
    public boolean contains( Object arg0 ){
        return indexOf(arg0) > -1;
    }

    @Override
    public boolean containsAll( Collection arg0 ){
        for ( Object obj : arg0 ) {
            if ( !contains( obj ) )
                return false;
        }
        return true;
    }

    @Override
    public Object get( int pos ){
        return get("" + pos);
    }
    
    @Override
    public Iterator iterator(){
        return new LazyBSONListIterator();
    }

    @Override
    public int indexOf( Object arg0 ){
        int pos = 0;
        Iterator it = iterator();
        while ( it.hasNext() ) {
            Object curr = it.next();
            if ( arg0.equals( curr ) )
                return pos;

            pos++;
        }
        return -1;
    }
    
    @Override
    public int lastIndexOf( Object arg0 ){
        int pos = 0;
        int lastFound = -1;
        
        Iterator it = iterator();
        while(it.hasNext()) {
            Object curr = it.next();
            if(arg0.equals( curr ))
                lastFound = pos;
            
            pos++;
        }
        
        return lastFound;
    }

    @Override
    public int size(){
        //TODO check the last one and get the key/field name to see the ordinal position in case the array is stored with missing elements.
        return getElements().size();
    }

    public class LazyBSONListIterator implements Iterator {
        List<ElementRecord> elements;
        int pos=0;
        
        public LazyBSONListIterator() {
            elements = getElements();
        }
        
        @Override
        public boolean hasNext(){
            return pos < elements.size();
        }

        @Override
        public Object next(){
            return getElementValue(elements.get(pos++));
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException( "Read Only" );
        }
        
    }

    @Override
    public ListIterator listIterator( int arg0 ){
        throw new UnsupportedOperationException( "Not Supported" );
    }

    @Override
    public ListIterator listIterator(){
        throw new UnsupportedOperationException( "Not Supported" );
    }


    @Override
    public boolean add( Object arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public void add( int arg0 , Object arg1 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public boolean addAll( Collection arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public boolean addAll( int arg0 , Collection arg1 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public void clear(){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public boolean remove( Object arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public Object remove( int arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public boolean removeAll( Collection arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public boolean retainAll( Collection arg0 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public Object set( int arg0 , Object arg1 ){
        throw new UnsupportedOperationException( "Read Only" );
    }

    @Override
    public List subList( int arg0 , int arg1 ){
        throw new UnsupportedOperationException( "Not Supported" );
    }

    @Override
    public Object[] toArray(){
        throw new UnsupportedOperationException( "Not Supported" );
    }

    @Override
    public Object[] toArray( Object[] arg0 ){
        throw new UnsupportedOperationException( "Not Supported" );
    }

}
