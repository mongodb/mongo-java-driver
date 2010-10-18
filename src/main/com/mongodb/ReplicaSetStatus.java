// ReplicaSetStatus.java

package com.mongodb;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
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
    
    ReplicaSetStatus( Mongo m , List<ServerAddress> initial , DBConnector connector ){
        _mongo = m;
        
        if ( connector == null )
            _adminDB = m.getDB( "admin" );
        else
            _adminDB = new DBApiLayer( m , "admin" , connector );

        _all = new CopyOnWriteArrayList<Node>();
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
        for ( Node n : _all ) {
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
        // since we iterate over all nodes to find out the one with the best ping, no random magic should be necessary
        for ( Node n : _all ) {

            if ( ! n.secondary() )
                continue;
            
            if ( best == null ){
                best = n;
                continue;
            }
            
            long diff = best._pingTime - n._pingTime;
            if ( diff > 15 )
                best = n;
        }
        
        if ( best == null )
            return null;
        return best._addr;
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
                CommandResult res = _port.runCommand( _adminDB , _isMasterCmd );
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

                if ( _isMaster ){
                	// check the master node for active hosts
                	if ( res.containsField( "hosts" ) ){
                		Set<Node> tmpNodeList = new HashSet<Node>();
                        for ( Object x : (List)res.get("hosts") ){
                            tmpNodeList.add(_addIfNotHere( x.toString() ));
                        }
                        // see if we can remove something ( maybe the mongodb admin removed a node )
                        if (! tmpNodeList.containsAll(_all) ) {
                        	for ( Node n : _all ) {
                        		if (! tmpNodeList.contains(n) ) {
                        			_all.remove(n);
                        		}
                        	}
                        }
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

            if ( ! _isMaster )
                return;
            
            try {
                DBObject config = _port.findOne( _mongo.getDB( "local" ) , "system.replset" , new BasicDBObject() );
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
        
        public boolean equals(Object obj) {
        	if ( obj instanceof Node ) {
        		Node other = (Node) obj;
        		return other._addr.equals(_addr);
        	}
        	return false;
        }
        
        public int hashCode() {
        	return _addr.hashCode();
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
    	for ( Node n : _all ) {
            n.update();
        }        
    }

    Node _addIfNotHere( String host ){
        Node n = findNode( host );
        if ( n == null ){
            try {
                _all.add( n = new Node( new ServerAddress( host ) ) );
            }
            catch ( UnknownHostException un ){
                _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            }
        }
        return n;
    }

    Node findNode( String host ){
    	for ( Node n : _all )
            if ( n._names.contains( host ) )
                return n;
        
        ServerAddress addr = null;
        try {
            addr = new ServerAddress( host );
        }
        catch ( UnknownHostException un ){
            _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            return null;
        }

        for ( Node n : _all ) {
            if ( n._addr.equals( addr ) ){
                n._names.add( host );
                return n;
            }
        }

        return null;
    }
    
    void printStatus(){
    	System.out.println( _all );
    }

    void close(){
        _closed = true;
    }

    final Mongo _mongo;
    final DB _adminDB;

    final List<Node> _all;
    Updater _updater;
    String _setName = null; // null until init
    Logger _logger = _rootLogger; // will get changed to use set name once its found

    String _lastPrimarySignal;
    boolean _closed = false;
    
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

        ReplicaSetStatus status = new ReplicaSetStatus( m , addrs , null );
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
