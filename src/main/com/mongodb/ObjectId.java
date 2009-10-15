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

package com.mongodb;

import java.util.*;
import java.nio.*;

import com.mongodb.util.*;

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
 */
public class ObjectId implements Comparable<ObjectId>{

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

        if ( babble ){
            String baseString = s.substring( 0 , 16 );
            String incString = s.substring( 16 );
            
            ByteBuffer buf = ByteBuffer.allocate(24);
            
            for (int i=0; i < baseString.length() / 2; i++) {
                buf.put((byte) Integer.parseInt(baseString.substring(i*2, i*2 + 2), 16));
            }
            
            buf.flip();
            
            _base = buf.getLong();
            
            buf.clear();
            
            for (int i=0; i < incString.length() / 2; i++) {
                buf.put((byte) Integer.parseInt(incString.substring(i*2, i*2 + 2), 16));
            }
            
            buf.flip();
            
            _inc = buf.getInt();
        }
        else {
            byte b[] = new byte[12];
            for ( int i=0; i<b.length; i++ ){
                b[b.length-(i+1)] = (byte)Integer.parseInt( s.substring( i*2 , i*2 + 2) , 16 );
            }
            ByteBuffer bb = ByteBuffer.wrap( b );

            _inc = bb.getInt();            
            _base = bb.getLong();
        }
        _new = false;
    }

    public ObjectId( byte[] b ){
        if ( b.length != 12 )
            throw new IllegalArgumentException( "need 12 bytes" );
        reverse( b );
        ByteBuffer bb = ByteBuffer.wrap( b );
        
        _inc = bb.getInt();            
        _base = bb.getLong();
    }
    
    
    ObjectId( long base , int inc ){
        _base = base;
        _inc = inc;
        
        _new = false;
    }
    
    /** Create a new object id.
     */
    public ObjectId(){
        _base = ( ((long)_time) << 32) | _machine;
        
        if ( D ) System.out.println( "base : " + Long.toHexString( _base ) );
        
        synchronized ( _incLock ){
            if ( _nextShort == Short.MAX_VALUE )
                _nextByte++;

            int myb = ( ((int)_nextByte) << 16 ) & 0xFF0000;
            int myi = ( _nextShort++ ) & 0xFFFF;
            
            _inc = myb | myi;
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
            _base == other._base && 
            _inc == other._inc;
    }

    public String toStringBabble(){
        String a = Long.toHexString( _base );
        String b = Integer.toHexString( _inc );
        
        StringBuilder buf = new StringBuilder( 16 );

        for ( int i=0; i<(16-a.length()); i++ )
            buf.append( "0" );
        buf.append( a );

        for ( int i=0; i<(8-b.length()); i++ )
            buf.append( "0" );
        buf.append( b );
        
        return buf.toString();
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
        bb.putLong( _base );
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
        
        if ( id._base == _base ){
            if ( _inc < id._inc )
                return -1;
            if ( _inc > id._inc )
                return 1;
            return 0;
        }

        if ( _base < id._base )
            return -1;

        if ( _base > id._base )
            return 1;

        return 0;
    }

    public long getBase(){
        return _base;
    }
    
    public int getInc(){
        return _inc;
    }

    final long _base;
    final int _inc;
    
    boolean _new;
    
    private static byte _nextByte = (byte)(new java.util.Random()).nextInt();
    private static short _nextShort = (short)(new java.util.Random()).nextInt();
    private static final String _incLock = new String( "ObjectId._incLock" );

    private static int _time = (int)(System.currentTimeMillis()/1000);
    
    static final Thread _timeFixer;
    private static final long _machine;
    private static final int _bottomTop;
    static {
        try {
            int startTime = (int)( java.lang.management.ManagementFactory.getRuntimeMXBean().getStartTime() & 0xFFFF );
            _bottomTop = ( startTime & 0xFF ) << 24;
            if ( D ) System.out.println( "top of last piece : " + Integer.toHexString( _bottomTop ) );
            int machinePiece = ( java.net.InetAddress.getLocalHost().getHostName().hashCode() & 0xFFFFFF ) << 8;
            _machine = ( ( startTime >> 8 ) | machinePiece ) & 0x7FFFFFFF;
            if ( D ) System.out.println( "machine piece : " + Long.toHexString( _machine ) );
        }
        catch ( java.io.IOException ioe ){
            throw new RuntimeException( ioe );
        }

        _timeFixer = new Thread("ObjectId-TimeFixer"){
                public void run(){
                    while ( true ){
                        ThreadUtil.sleep( 499 );
                        _time = (int)(System.currentTimeMillis()/1000);
                    }
                }
            };
        _timeFixer.setDaemon( true );
        _timeFixer.start();
    }

    public static void main( String args[] ){
        Set<ObjectId> s = new HashSet<ObjectId>();
        while ( true ){
            ObjectId i = get();
            if ( s.contains( i ) )
                throw new RuntimeException( "ObjectId() generated a repeat" );
            s.add( i );

            ObjectId o = new ObjectId( i.toString() );
            if ( ! i.equals( o ) )
                throw new RuntimeException( o.toString() + " != " + i.toString() );
        }

    }

}
