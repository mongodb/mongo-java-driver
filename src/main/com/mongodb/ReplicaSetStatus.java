// ReplicaSetStatus.java

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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.util.JSON;

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
public class ReplicaSetStatus {

	static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );
    static final int UNAUTHENTICATED_ERROR_CODE = 10057;

    ReplicaSetStatus( Mongo mongo, List<ServerAddress> initial ){
        _mongo = mongo;
        _all = Collections.synchronizedList( new ArrayList<Node>() );
        for ( ServerAddress addr : initial ){
            _all.add( new Node( addr ) );
        }
        _nextResolveTime = System.currentTimeMillis() + inetAddrCacheMS;

        _updater = new Updater();
    }

    void start() {
        _updater.start();
    }
    
    boolean ready(){
        return _setName != null;
    }

    public String getName() {
        return _setName;
    }

    @Override
	public String toString() {
	StringBuffer sb = new StringBuffer();
	sb.append("{replSetName: '" + _setName );
	sb.append("', closed:").append(_closed).append(", ");
	sb.append("nextResolveTime:'").append(new Date(_nextResolveTime).toString()).append("', ");
	sb.append("members : [ ");
	if(_all != null) {
		for(Node n : _all)
			sb.append(n.toJSON()).append(",");
		sb.setLength(sb.length()-1); //remove last comma
	}
	sb.append("] ");

	return sb.toString();
	}

    void _checkClosed(){
        if ( _closed )
            throw new IllegalStateException( "ReplicaSetStatus closed" );
    }

    /**
     * @return master or null if don't have one
     */
    public ServerAddress getMaster(){
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
	 * @param srv
	 *            the server to compare
	 * @return indication if the ServerAddress is the current Master/Primary
	 */
	public boolean isMaster(ServerAddress srv) {
		if (srv == null)
			return false;

		return srv.equals(getMaster());
	}

    /**
     * @return a good secondary by tag value or null if can't find one
     */
    ServerAddress getASecondary( DBObject tags ){
        for ( String key : tags.keySet() ) {
            System.err.println( "Searching for a secondary with tag " + key + " = " + tags.get( key ) );
            ServerAddress secondary = getASecondary( key, tags.get( key ).toString() );
            System.err.println( "Secondary retrieved: " + secondary );
            if (secondary != null)
                return secondary;
        }
        // No matching server for any supplied tags was found
        return null;
    }

    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary(){
        return getASecondary( null, null );
    }
    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary( String tagKey, String tagValue ){
        _checkClosed();
        Node best = null;
        double badBeforeBest = 0;

        if (tagKey == null && tagValue != null || tagValue == null & tagKey != null)
           throw new IllegalArgumentException( "Tag Key & Value must be consistent: both defined or not defined." );

        int start = _random.nextInt( _all.size() );

        double mybad = 0;

        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get( ( start + i ) % _all.size() );

            if ( ! n.secondary() ){
                System.err.println( " not a secondary: " + n );
                mybad++;
                continue;
            } else if (tagKey != null && !n.checkTag( tagKey, tagValue )){
                System.err.println( " Doesn't have tag " + tagKey + " = " + tagValue
                                   + " ... " + n);
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
            if ( diff > slaveAcceptableLatencyMS ||
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

    boolean hasServerUp() {
        for (int i = 0; i < _all.size(); i++) {
            Node n = _all.get(i);
            if (n._ok) {
                return true;
            }
        }
        return false;
    }

    class Node {

        Node( ServerAddress addr ){
            _addr = addr;
            _port = new DBPort( addr , null , _mongoOptions );
            _names.add( addr.toString() );
        }

        private void updateAddr() {
            try {
                if (_addr.updateInetAddr()) {
                    // address changed, need to use new ports
                    _port = new DBPort(_addr, null, _mongoOptions);
                    _mongo.getConnector().updatePortPool(_addr);
                    _logger.log(Level.INFO, "Address of host " + _addr.toString() + " changed to " + _addr.getSocketAddress().toString());
                }
            } catch (UnknownHostException ex) {
                _logger.log(Level.WARNING, null, ex);
            }
        }

        synchronized void update(){
            update(null);
        }

        synchronized void update(Set<Node> seenNodes){
            try {
                long start = System.currentTimeMillis();
                CommandResult res = _port.runCommand( _mongo.getDB("admin") , _isMasterCmd );
                _lastCheck = System.currentTimeMillis();
                _pingTime = _lastCheck - start;

                if ( res == null ){
                    throw new MongoInternalException("Invalid null value returned from isMaster");
                }

                if (!_ok) {
                    _logger.log( Level.INFO , "Server seen up: " + _addr );
                }
                _ok = true;
                _isMaster = res.getBoolean( "ismaster" , false );
                _isSecondary = res.getBoolean( "secondary" , false );
                _lastPrimarySignal = res.getString( "primary" );

                if ( res.containsField( "hosts" ) ){
                    for ( Object x : (List)res.get("hosts") ){
                        String host = x.toString();
                        Node node = _addIfNotHere(host);
                        if (node != null && seenNodes != null)
                            seenNodes.add(node);
                    }
                }

                if ( res.containsField( "passives" ) ){
                    for ( Object x : (List)res.get("passives") ){
                        String host = x.toString();
                        Node node = _addIfNotHere(host);
                        if (node != null && seenNodes != null)
                            seenNodes.add(node);
                    }
                }

                // Tags were added in 2.0 but may not be present
                if (res.containsField( "tags" )) {
                    DBObject tags = (DBObject) res.get( "tags" );
                    for ( String key : tags.keySet() ) {
                        _tags.put( key, tags.get( key ).toString() );
                    }
                }

                if (_isMaster ) {
                    // max size was added in 1.8
                    if (res.containsField("maxBsonObjectSize"))
                        maxBsonObjectSize = ((Integer)res.get( "maxBsonObjectSize" )).intValue();
                    else
                        maxBsonObjectSize = Bytes.MAX_OBJECT_SIZE;
                }

                if (res.containsField("setName")) {
	                String setName = res.get( "setName" ).toString();
	                if ( _setName == null ){
	                    _setName = setName;
	                    _logger = Logger.getLogger( _rootLogger.getName() + "." + setName );
	                }
	                else if ( !_setName.equals( setName ) ){
	                    _logger.log( Level.SEVERE , "mis match set name old: " + _setName + " new: " + setName );
	                    return;
	                }
                }

            }
            catch ( Exception e ){
                if (_ok == true) {
                    _logger.log( Level.WARNING , "Server seen down: " + _addr, e );
                } else if (Math.random() < 0.1) {
                    _logger.log( Level.WARNING , "Server seen down: " + _addr );
                }
                _ok = false;
            }

            if ( ! _isMaster )
                return;
        }

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
        }

        public boolean checkTag(String key, String value){
            return _tags.containsKey( key ) && _tags.get( key ).equals( value );
        }

        public String toString(){
            StringBuilder buf = new StringBuilder();
            buf.append( "Replica Set Node: " ).append( _addr ).append( "\n" );
            buf.append( "\t ok \t" ).append( _ok ).append( "\n" );
            buf.append( "\t ping \t" ).append( _pingTime ).append( "\n" );

            buf.append( "\t master \t" ).append( _isMaster ).append( "\n" );
            buf.append( "\t secondary \t" ).append( _isSecondary ).append( "\n" );

            buf.append( "\t priority \t" ).append( _priority ).append( "\n" );

            buf.append( "\t tags \t" ).append( JSON.serialize( _tags )  ).append( "\n" );

            return buf.toString();
        }

        public String toJSON(){
            StringBuilder buf = new StringBuilder();
            buf.append( "{ address:'" ).append( _addr ).append( "', " );
            buf.append( "ok:" ).append( _ok ).append( ", " );
            buf.append( "ping:" ).append( _pingTime ).append( ", " );
            buf.append( "isMaster:" ).append( _isMaster ).append( ", " );
            buf.append( "isSecondary:" ).append( _isSecondary ).append( ", " );
            buf.append( "priority:" ).append( _priority ).append( ", " );
            if(_tags != null && !_tags.isEmpty())
		buf.append( "tags:" ).append( JSON.serialize( _tags )  );
            buf.append("}");

            return buf.toString();
        }

        public void close() {
            _port.close();
            _port = null;
        }
        
        final ServerAddress _addr;
        final Set<String> _names = Collections.synchronizedSet( new HashSet<String>() );
        DBPort _port; // we have our own port so we can set different socket options and don't have to owrry about the pool
        final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>( );

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

                    long now = System.currentTimeMillis();
                    if (inetAddrCacheMS > 0 && _nextResolveTime < now) {
                        _nextResolveTime = now + inetAddrCacheMS;
                        for (Node node : _all) {
                            node.updateAddr();
                        }
                    }

                    // force check on master
                    // otherwise master change may go unnoticed for a while if no write concern
                    _mongo.getConnector().checkMaster(true, false);
                }
                catch ( Exception e ){
                    _logger.log( Level.WARNING , "couldn't do update pass" , e );
                }

                try {
                    Thread.sleep( updaterIntervalMS );
                }
                catch ( InterruptedException ie ){
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
            if (n != null) {
                n.update();
                if ( n._isMaster )
                    return n;
            }
        }

        updateAll();
        return getMasterNode();
    }

    synchronized void updateAll(){
        HashSet<Node> seenNodes = new HashSet<Node>();
        for ( int i=0; i<_all.size(); i++ ){
            Node n = _all.get(i);
            n.update(seenNodes);
        }

        if (!seenNodes.isEmpty()) {
            // not empty, means that at least 1 server gave node list
            // remove unused hosts
            Iterator<Node> it = _all.iterator();
            while (it.hasNext()) {
                if (!seenNodes.contains(it.next()))
                    it.remove();
            }
        }
    }

    List<ServerAddress> getServerAddressList() {
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        for (Node node : _all)
            addrs.add(node._addr);
        return addrs;
    }

    Node _addIfNotHere( String host ){
        Node n = findNode( host );
        if ( n == null ){
            try {
                n = new Node( new ServerAddress( host ) );
                _all.add( n );
            }
            catch ( UnknownHostException un ){
                _logger.log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            }
        }
        return n;
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
        if (!_closed) {
            _closed = true;
            for (int i = 0; i < _all.size(); i++) {
                _all.get(i).close();
            }
        }
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    public int getMaxBsonObjectSize() {
        return maxBsonObjectSize;
    }

    final List<Node> _all;
    Updater _updater;
    Mongo _mongo;
    String _setName = null; // null until init
    int maxBsonObjectSize = 0;
    Logger _logger = _rootLogger; // will get changed to use set name once its found

    String _lastPrimarySignal;
    boolean _closed = false;

    final Random _random = new Random();
    long _nextResolveTime;

    static int updaterIntervalMS;
    static int slaveAcceptableLatencyMS;
    static int inetAddrCacheMS;

    static final MongoOptions _mongoOptions = new MongoOptions();

    static {
        updaterIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        inetAddrCacheMS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
        _mongoOptions.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        _mongoOptions.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
    }

    static final DBObject _isMasterCmd = new BasicDBObject( "ismaster" , 1 );

    public static void main( String args[] )
        throws Exception {
        List<ServerAddress> addrs = new LinkedList<ServerAddress>();
        addrs.add( new ServerAddress( "127.0.0.1" , 27018 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27019 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27020 ) );
        addrs.add( new ServerAddress( "127.0.0.1" , 27021 ) );

        Mongo m = new Mongo( addrs );

        ReplicaSetStatus status = new ReplicaSetStatus( m, addrs );
        status.start();
        System.out.println( status.ensureMaster()._addr );

        while ( true ){
            System.out.println( status.ready() );
            if ( status.ready() ){
                status.printStatus();
                System.out.println( "master: " + status.getMaster() + "\t secondary: " + status.getASecondary() );
            }
            System.out.println( "-----------------------" );
            DBObject tags = new BasicDBObject();
            tags.put( "dc", "newyork" );
            System.out.println( "Tagged Node: " + status.getASecondary( tags ) );
            Thread.sleep( 5000 );
        }

    }
}
