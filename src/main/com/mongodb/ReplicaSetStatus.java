// ReplicaSetStatus.java

package com.mongodb;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;

/**
 * keeps replica set status
 * has a background thread to ping so it stays current
 * 
 * TODO
 *  pull config to get
 *      priority
 *      slave delay
 *      set name
 *      tags (when we do it)
 */
class ReplicaSetStatus {

    static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );
    
    ReplicaSetStatus( Mongo m , List<ServerAddress> initial ){
        _mongo = m;
        _adminDB = _mongo.getDB( "admin" );

        _all = Collections.synchronizedList( new ArrayList<Node>() );
        for ( ServerAddress addr : initial ){
            _all.add( new Node( addr ) );
        }

        _updater = new Updater();
        _updater.start();
    }

    boolean ready(){
        return _setName != null;
    }

    class Node {
        
        Node( ServerAddress addr ){
            _addr = addr;
            _port = new DBPort( addr.getSocketAddress() , null , _mongoOptions );
            _names.add( addr.toString() );
        }

        synchronized void update(){
            try {
                long start = System.currentTimeMillis();
                CommandResult res = _port.runCommand( _adminDB , DBTCPConnector._isMaster );
                _lastCheck = System.currentTimeMillis();
                _pingTime = _lastCheck - start;
                
                if ( res == null ){
                    _ok = false;
                    return;
                }
                
                _ok = true;
                _isMaster = res.getBoolean( "ismaster" , false );
                _isSecondary = res.getBoolean( "secondary" , false );
                
                if ( res.containsKey( "hosts" ) ){
                    for ( Object x : (List)res.get("hosts") ){
                        _addIfNotHere( x.toString() );
                    }
                }
                
                if ( _isMaster ){
                    DBObject config = _port.findOne( _mongo.getDB( "local" ) , "system.replset" , new BasicDBObject() );
                    if ( config == null ){
                        _logger.log( Level.SEVERE , "no replset config!" );
                    }
                    else {
                        
                        String setName = config.get( "_id" ).toString();
                        if ( _setName == null ){
                            _setName = setName;
                            _logger = Logger.getLogger( _rootLogger.getName() + "." + setName );
                        }
                        else if ( !_setName.equals( setName ) ){
                            _logger.log( Level.SEVERE , "mis match set name old: " + _setName + " new: " + setName );
                            return;
                        } 

                        // TODO: look at members
                    }
                }
                
            }
            catch ( MongoInternalException e ){
                Throwable root = e;
                if ( e.getCause() != null )
                    root = e.getCause();
                _logger.log( Level.WARNING , "node down: " + _addr + " " + root );
                _ok = false;
            }
            catch ( Exception e ){
                _logger.log( Level.SEVERE , "can't update node: " + _addr , e );
                _ok = false;
            }
                
        }

        public String toString(){
            StringBuilder buf = new StringBuilder();
            buf.append( "Replica Set Node: " ).append( _addr ).append( "\n" );
            buf.append( "\t ok \t" ).append( _ok ).append( "\n" );
            buf.append( "\t ping \t" ).append( _pingTime ).append( "\n" );

            buf.append( "\t master \t" ).append( _isMaster ).append( "\n" );
            buf.append( "\t secondary \t" ).append( _isSecondary ).append( "\n" );

            buf.append( "\t priority \t" ).append( _priority ).append( "\n" );

            return buf.toString();
        }
        
        
        final ServerAddress _addr;
        final Set<String> _names = Collections.synchronizedSet( new HashSet<String>() );
        final DBPort _port; // we have our own port so we can set different socket options and don't have to owrry about the pool

        boolean _ok = false;        
        long _lastCheck = 0;
        long _pingTime = 0;

        boolean _isMaster = false;
        boolean _isSecondary = false;

        double _priority = 0;
    }
    
    class Updater extends Thread {
        Updater(){
            super( "ReplicaSetStatus:Updater" );
            setDaemon( true );
        }
        
        public void run(){
            while ( true ){
                try {
                    for ( int i=0; i<_all.size(); i++ ){
                        Node n = _all.get(i);
                        n.update();
                    }
                }
                catch ( Exception e ){
                    _logger.log( Level.WARNING , "couldn't do update pass" , e );
                }
                
                try {
                    Thread.sleep( 5 * 1000 );
                }
                catch ( InterruptedException ie ){
                    // TODO: maybe something smarter
                }

            }
        }
    }

    void _addIfNotHere( String host ){
        for ( int i=0; i<_all.size(); i++ )
            if ( _all.get(i)._names.contains( host ) )
                return;
        
        ServerAddress addr = null;
        try {
            addr = new ServerAddress( host );
        }
        catch ( UnknownHostException un ){
            _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            return;
        }

        for ( int i=0; i<_all.size(); i++ ){
            if ( _all.get(i)._addr.equals( addr ) ){
                _all.get(i)._names.add( host );
                return;        
            }
        }

        _all.add( new Node( addr ) );
    }
    
    void printStatus(){
        for ( int i=0; i<_all.size(); i++ )
            System.out.println( _all.get(i) );
    }

    final Mongo _mongo;
    final DB _adminDB;

    final List<Node> _all;
    Updater _updater;
    String _setName = null; // null until init
    Logger _logger = _rootLogger; // will get changed to use set name once its found

    static final MongoOptions _mongoOptions = new MongoOptions();
    static {
        _mongoOptions.connectTimeout = 20000;
        _mongoOptions.socketTimeout = 20000;
    }

    public static void main( String args[] )
        throws Exception {
        List<ServerAddress> addrs = new LinkedList<ServerAddress>();
        addrs.add( new ServerAddress( "localhost" , 27017 ) );
        addrs.add( new ServerAddress( "localhost" , 27018 ) );

        Mongo m = new Mongo( addrs );

        ReplicaSetStatus status = new ReplicaSetStatus( m , addrs );

        while ( true ){
            System.out.println( status.ready() );
            if ( status.ready() )
                status.printStatus();
            System.out.println( "-----------------------" );
            Thread.sleep( 5000 );
        }

    }
}
