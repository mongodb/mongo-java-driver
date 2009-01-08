// ObjectId.java

package com.mongodb;

import java.util.*;

import com.mongodb.util.*;

/**
 * 12 bytes
 * ---
 *  0 time
 *  1
 *  2
 *  3 
 *  4 machine
 *  5 
 *  6 
 *  7 pid
 *  8 
 *  9 inc
 * 10
 * 11
 */
public class ObjectId implements Comparable<ObjectId>{

    static final boolean D = false;
    
    public static ObjectId get(){
        return new ObjectId();
    }
    
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
    
    /**
     * @return an ObjectId if it can be massages, null otherwise.  if you pass in null get null 
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

    public ObjectId( String s ){

        if ( ! isValid( s ) )
            throw new IllegalArgumentException( "invalid ObjectId [" + s + "]" );

        String baseString = s.substring( 0 , 16 );
        String incString = s.substring( 16 );

        _base = Long.parseLong( baseString , 16 );
        _inc = Integer.parseInt( incString , 16 );

        _new = false;
    }
    
    ObjectId( long base , int inc ){
        _base = base;
        _inc = inc;
        
        _new = false;
    }
    
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

    public String toString(){
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
                throw new RuntimeException( "fuck" );
            s.add( i );

            ObjectId o = new ObjectId( i.toString() );
            if ( ! i.equals( o ) )
                throw new RuntimeException( o.toString() + " != " + i.toString() );
        }

    }

}
