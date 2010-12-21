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
 *      tags (when we do it)
 */
class ReplicaSetStatus {

    static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );
    
    ReplicaSetStatus( List<ServerAddress> initial ){
        
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

    void _checkClosed(){
        if ( _closed )
            throw new IllegalStateException( "ReplicaSetStatus closed" );
    }

    /**
     * @return master or null if don't have one
     */
    ServerAddress getMaster(){
        Node n = getMasterNode();
        if ( n == null )
            return null;
        return n._addr;
    }

    Node getMasterNode(){
        _checkClosed();
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get(i);
            if ( n.master() )
                return n;
        }
        return null;
    }

    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary(){
        _checkClosed();
        Node best = null;
        double badBeforeBest = 0;
        
        int start = _random.nextInt( _all.size() );
        
        double mybad = 0;
        
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get( ( start + i ) % _all.size() );
            
            if ( ! n.secondary() ){
                mybad++;
                continue;
            }
            
            if ( best == null ){
                best = n;
                badBeforeBest = mybad;
                mybad = 0;
                continue;
            }
            
            long diff = best._pingTime - n._pingTime;
            if ( diff > 15 || 
                 // this is a complex way to make sure we get a random distribution of slaves
                 ( ( badBeforeBest - mybad ) / ( _all.size() - 1 ) ) > _random.nextDouble() ) 
                { 
                best = n;
                badBeforeBest = mybad;
                mybad = 0;
            }

        }
        
        if ( best == null )
            return null;
        return best._addr;
    }

    class Node {
        
        Node( ServerAddress addr ){
            _addr = addr;
            _port = new DBPort( addr , null , _mongoOptions );
            _names.add( addr.toString() );
        }

        synchronized void update(){
            try {
                long start = System.currentTimeMillis();
                CommandResult res = _port.runCommand( "admin" , _isMasterCmd );
                _lastCheck = System.currentTimeMillis();
                _pingTime = _lastCheck - start;
                
                if ( res == null ){
                    _ok = false;
                    return;
                }
                
                _ok = true;
                _isMaster = res.getBoolean( "ismaster" , false );
                _isSecondary = res.getBoolean( "secondary" , false );
                _lastPrimarySignal = res.getString( "primary" );

                if ( res.containsKey( "hosts" ) ){
                    for ( Object x : (List)res.get("hosts") ){
                        _addIfNotHere( x.toString() );
                    }
                }
                
            }
            catch ( MongoInternalException e ){
                Throwable root = e;
                if ( e.getCause() != null )
                    root = e.getCause();
                _logger.log( Level.FINE , "node down: " + _addr + " " + root );
                _ok = false;
            }
            catch ( Exception e ){
                _logger.log( Level.SEVERE , "can't update node: " + _addr , e );
                _ok = false;
            }

            if ( ! _isMaster )
                return;
            
            try {
                DBObject config = _port.findOne( "local.system.replset" , new BasicDBObject() );
                if ( config == null ){
                    // probbaly a replica pair
                    // TODO: add this in when pairs are really gone
                    //_logger.log( Level.SEVERE , "no replset config!" );
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

            catch ( MongoInternalException e ){
                if ( _setName != null ){
                    // this probably means the master is busy, so going to ignore
                }
                else {
                    _logger.log( Level.SEVERE , "can't get intial config from node: " + _addr , e );
                }
            }
            catch ( Exception e ){
                _logger.log( Level.SEVERE , "unexpected error getting config from node: " + _addr , e );
            }

                
        }

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
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
            while ( ! _closed ){
                try {
                    updateAll();
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

    Node ensureMaster(){
        Node n = getMasterNode();
        if ( n != null ){
            n.update();
            if ( n._isMaster )
                return n;
        }

        if ( _lastPrimarySignal != null ){
            n = findNode( _lastPrimarySignal );
            n.update();
            if ( n._isMaster )
                return n;
        }
        
        updateAll();
        return getMasterNode();
    }

    void updateAll(){
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get(i);
            n.update();
        }        
    }

    void _addIfNotHere( String host ){
        Node n = findNode( host );
        if ( n == null ){
            try {
                _all.add( new Node( new ServerAddress( host ) ) );
            }
            catch ( UnknownHostException un ){
                _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            }
        }
    }

    Node findNode( String host ){
        for ( int i=0; i<_all.size(); i++ )
            if ( _all.get(i)._names.contains( host ) )
                return _all.get(i);
        
        ServerAddress addr = null;
        try {
            addr = new ServerAddress( host );
        }
        catch ( UnknownHostException un ){
            _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            return null;
        }

        for ( int i=0; i<_all.size(); i++ ){
            if ( _all.get(i)._addr.equals( addr ) ){
                _all.get(i)._names.add( host );
                return _all.get(i);
            }
        }

        return null;
    }
    
    void printStatus(){
        for ( int i=0; i<_all.size(); i++ )
            System.out.println( _all.get(i) );
    }

    void close(){
        _closed = true;
    }


    final List<Node> _all;
    Updater _updater;
    String _setName = null; // null until init
    Logger _logger = _rootLogger; // will get changed to use set name once its found

    String _lastPrimarySignal;
    boolean _closed = false;
    
    final Random _random = new Random();

    static final MongoOptions _mongoOptions = new MongoOptions();
    static {
        _mongoOptions.connectTimeout = 20000;
        _mongoOptions.socketTimeout = 20000;
    }

    static final DBObject _isMasterCmd = new BasicDBObject( "ismaster" , 1 );

    public static void main( String args[] )
        throws Exception {
        List<ServerAddress> addrs = new LinkedList<ServerAddress>();
        addrs.add( new ServerAddress( "localhost" , 27017 ) );
        addrs.add( new ServerAddress( "localhost" , 27018 ) );

        Mongo m = new Mongo( addrs );

        ReplicaSetStatus status = new ReplicaSetStatus( addrs );
        System.out.println( status.ensureMaster()._addr );

        while ( true ){
            System.out.println( status.ready() );
            if ( status.ready() ){
                status.printStatus();
                System.out.println( "master: " + status.getMaster() + "\t secondary: " + status.getASecondary() );
            }
            System.out.println( "-----------------------" );
            Thread.sleep( 5000 );
        }
        
    }
}
