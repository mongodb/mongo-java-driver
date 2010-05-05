// ObjectId.java

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

import java.util.*;
import java.nio.*;
import java.net.*;

/**
 * A globally unique identifier for objects.
 * <p>Consists of 12 bytes, divided as follows:
 * <blockquote><pre>
 * <table border="1">
 * <tr><td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td>
 *     <td>7</td><td>8</td><td>9</td><td>10</td><td>11</td></tr>
 * <tr><td colspan="4">time</td><td colspan="3">machine</td>
 *     <td colspan="2">pid</td><td colspan="3">inc</td></tr>
 * </table>
 * </pre></blockquote>
 * 
 * @dochub objectids
 */
public class ObjectId implements Comparable<ObjectId> , java.io.Serializable {

    static final boolean D = false;
    
    /** Gets a new object id.
     * @return the new id
     */
    public static ObjectId get(){
        return new ObjectId();
    }
    
    /** Checks if a string could be an <code>ObjectId</code>.
     * @return whether the string could be an object id
     */
    public static boolean isValid( String s ){
        if ( s == null )
            return false;
        
        if ( s.length() < 18 )
            return false;

        for ( int i=0; i<s.length(); i++ ){
            char c = s.charAt( i );
            if ( c >= '0' && c <= '9' )
                continue;
            if ( c >= 'a' && c <= 'f' )
                continue;
            if ( c >= 'A' && c <= 'F' )
                continue;

            return false;
        }        

        return true;
    }
    
    /** Turn an object into an <code>ObjectId</code>, if possible.
     * Strings will be converted into <code>ObjectId</code>s, if possible, and <code>ObjectId</code>s will
     * be cast and returned.  Passing in <code>null</code> returns <code>null</code>.
     * @param o the object to convert 
     * @return an <code>ObjectId</code> if it can be massaged, null otherwise 
     */
    public static ObjectId massageToObjectId( Object o ){
        if ( o == null )
            return null;
        
        if ( o instanceof ObjectId )
            return (ObjectId)o;

        if ( o instanceof String ){
            String s = o.toString();
            if ( isValid( s ) )
                return new ObjectId( s );
        }
        
        return null;
    }

    public ObjectId( Date time ){
        _time = _flip( (int)(time.getTime() / 1000) );
        _machine = _genmachine;
        synchronized ( _incLock ){
            _inc = _nextInc++;
        }     
        _new = false;
    }

    public ObjectId( Date time , int inc ){
        _time = _flip( (int)(time.getTime() / 1000) );
        _machine = _genmachine;
        _inc = inc;
        _new = false;
    }


    /** Creates a new instance from a string.
     * @param s the string to convert
     * @throws IllegalArgumentException if the string is not a valid id
     */
    public ObjectId( String s ){
        this( s , false );
    }

    public ObjectId( String s , boolean babble ){

        if ( ! isValid( s ) )
            throw new IllegalArgumentException( "invalid ObjectId [" + s + "]" );

        if ( babble )
            s = babbleToMongod( s );
        
        byte b[] = new byte[12];
        for ( int i=0; i<b.length; i++ ){
            b[b.length-(i+1)] = (byte)Integer.parseInt( s.substring( i*2 , i*2 + 2) , 16 );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
        
        _inc = bb.getInt(); 
        _machine = bb.getInt();
        _time = bb.getInt();

        _new = false;
    }

    public ObjectId( byte[] b ){
        if ( b.length != 12 )
            throw new IllegalArgumentException( "need 12 bytes" );
        reverse( b );
        ByteBuffer bb = ByteBuffer.wrap( b );
        
        _inc = bb.getInt();            
        _machine = bb.getInt();
        _time = bb.getInt();
    }
    
    
    public ObjectId( int time , int machine , int inc ){
        _time = time;
        _machine = machine;
        _inc = inc;
        
        _new = false;
    }
    
    /** Create a new object id.
     */
    public ObjectId(){
        _time = _gentime;
        _machine = _genmachine;
        
        synchronized ( _incLock ){
            _inc = _nextInc++;
        }
        
        _new = true;
    }

    public int hashCode(){
        return _inc;
    }

    public boolean equals( Object o ){
        
        if ( this == o )
            return true;

        ObjectId other = massageToObjectId( o );
        if ( other == null )
            return false;
        
        return 
            _time == other._time && 
            _machine == other._machine && 
            _inc == other._inc;
    }

    public String toStringBabble(){
        return babbleToMongod( toStringMongod() );
    }

    public String toStringMongod(){
        byte b[] = toByteArray();

        StringBuilder buf = new StringBuilder(24);
        
        for ( int i=0; i<b.length; i++ ){
            int x = b[i] & 0xFF;
            String s = Integer.toHexString( x );
            if ( s.length() == 1 )
                buf.append( "0" );
            buf.append( s );
        }

        return buf.toString();
    }
    
    public byte[] toByteArray(){
        byte b[] = new byte[12];
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.putInt( _inc );
        bb.putInt( _machine );
        bb.putInt( _time );
        reverse( b );
        return b;
    }

    static void reverse( byte[] b ){
        for ( int i=0; i<b.length/2; i++ ){
            byte t = b[i];
            b[i] = b[ b.length-(i+1) ];
            b[b.length-(i+1)] = t;
        }
    }
    
    static String _pos( String s , int p ){
        return s.substring( p * 2 , ( p * 2 ) + 2 );
    }
    
    public static String babbleToMongod( String b ){
        if ( ! isValid( b ) )
            throw new IllegalArgumentException( "invalid object id: " + b );
        
        StringBuilder buf = new StringBuilder( 24 );
        for ( int i=7; i>=0; i-- )
            buf.append( _pos( b , i ) );
        for ( int i=11; i>=8; i-- )
            buf.append( _pos( b , i ) );
        
        return buf.toString();
    }

    public String toString(){
        return toStringMongod();
    }

    public int compareTo( ObjectId id ){
        if ( id == null )
            return -1;
        
        long xx = id.getTime() - getTime();
        if ( xx > 0 )
            return -1;
        else if ( xx < 0 )
            return 1;

        int x = id._machine - _machine;
        if ( x != 0 )
            return -x;

        x = id._inc - _inc;
        if ( x != 0 )
            return -x;

        return 0;
    }

    public int getMachine(){
        return _machine;
    }
    
    public long getTime(){
        long z = _flip( _time );
        return z * 1000;
    }

    public int getInc(){
        return _inc;
    }
    
    public int _time(){
        return _time;
    }
    public int _machine(){
        return _machine;
    }
    public int _inc(){
        return _inc;
    }

    public boolean isNew(){
        return _new;
    }

    public void notNew(){
        _new = false;
    }

    final int _time;
    final int _machine;
    final int _inc;
    
    boolean _new;

    static int _flip( int x ){
        if ( true ){
            byte b[] = new byte[4];
            ByteBuffer bb = ByteBuffer.wrap( b );
            bb.order( ByteOrder.LITTLE_ENDIAN );
            bb.putInt( x );
            bb.flip();
            bb.order( ByteOrder.BIG_ENDIAN );
            return bb.getInt();
        }
        int z = 0;
        z |= ( x & 0xFF ) << 24;
        z |= ( x & 0xFF00 ) << 8;
        z |= ( x & 0xFF00000 ) >> 8;
        z |= ( x & 0xFF000000 ) >> 24;
        return z;
    }
    
    private static int _nextInc = (new java.util.Random()).nextInt();
    private static final String _incLock = new String( "ObjectId._incLock" );

    private static int _gentime = _flip( (int)(System.currentTimeMillis()/1000) );
    
    static final Thread _timeFixer;
    private static final int _genmachine;
    static {

        try {
            
            final int machinePiece;
            {
                StringBuilder sb = new StringBuilder();
                Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
                while ( e.hasMoreElements() ){
                    NetworkInterface ni = e.nextElement();
                    sb.append( ni.toString() );
                }
                machinePiece = sb.toString().hashCode() << 16;
                if ( D ) System.out.println( "machine piece post: " + Integer.toHexString( machinePiece ) );
            }
            
            final int processPiece = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode() & 0xFFFF;
            if ( D ) System.out.println( "process piece: " + Integer.toHexString( processPiece ) );

            _genmachine = machinePiece | processPiece;
            if ( D ) System.out.println( "machine : " + Integer.toHexString( _genmachine ) );
        }
        catch ( java.io.IOException ioe ){
            throw new RuntimeException( ioe );
        }

        _timeFixer = new Thread("ObjectId-TimeFixer"){
                public void run(){
                    while ( true ){
                        try {
                            Thread.sleep( 499 );
                        }
                        catch ( Exception e ){}
                        _gentime = _flip( (int)(System.currentTimeMillis()/1000) );
                    }
                }
            };
        _timeFixer.setDaemon( true );
        _timeFixer.start();
    }

    public static void main( String args[] ){
        
        if ( true ){
            int z = _nextInc;
            System.out.println( Integer.toHexString( z ) );
            System.out.println( Integer.toHexString( _flip( z ) ) );
            System.out.println( Integer.toHexString( _flip( _flip( z ) ) ) );
            return;
        }

        ObjectId x = new ObjectId();

        double num = 5000000.0;
        
        long start = System.currentTimeMillis();
        for ( double i=0; i<num; i++ ){
            ObjectId id = get();
        }
        long end = System.currentTimeMillis();
        System.out.println( ( ( num * 1000.0 ) / ( end - start ) ) + " oid/sec" );
        
        Set<ObjectId> s = new HashSet<ObjectId>();
        for ( double i=0; i<num/10; i++ ){
            ObjectId id = get();
            if ( s.contains( id ) )
                throw new RuntimeException( "ObjectId() generated a repeat" );
            s.add( id );

            ObjectId o = new ObjectId( id.toString() );
            if ( ! id.equals( o ) )
                throw new RuntimeException( o.toString() + " != " + id.toString() );
        }

    }

}
