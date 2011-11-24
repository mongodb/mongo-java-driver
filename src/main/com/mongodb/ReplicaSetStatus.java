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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
        _mongoOptions = _mongoOptionsDefaults.copy();
        _mongoOptions.socketFactory = mongo._options.socketFactory;

        _mongo = mongo;
        _all = Collections.synchronizedList( new ArrayList<Node>() );
        for ( ServerAddress addr : initial ){
            _all.add( new Node( addr, _all,  _logger, _mongo, _mongoOptions, _maxBsonObjectSize, _setName, _lastPrimarySignal ) );
        }
        _nextResolveTime = System.currentTimeMillis() + inetAddrCacheMS;

        _updater = new Updater();
    }

    void start() {
        _updater.start();
    }

    boolean ready(){
        return _setName.get() != null;
    }

    public String getName() {
        return _setName.get();
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
            ServerAddress secondary = getASecondary( key, tags.get( key ).toString() );
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
    ServerAddress getASecondary( String tagKey, String tagValue ) {
        _checkClosed();
        return getASecondary(tagKey, tagValue, _all, _random);
    }

    /**
     * This was extracted so we can test the logic from a unit test. This can't be
     * tested from a standalone unit test until node is more decoupled from this class.
     *
     * @return a good secondary or null if can't find one
     */
    static ServerAddress getASecondary( final String pTagKey,
                                        final String pTagValue,
                                        final List<Node> pNodes,
                                        final Random pRandom)
    {
        Node best = null;
        double badBeforeBest = 0;

        if (pTagKey == null && pTagValue != null || pTagValue == null & pTagKey != null)
           throw new IllegalArgumentException( "Tag Key & Value must be consistent: both defined or not defined." );

        int start = pRandom.nextInt( pNodes.size() );

        final int nodeCount = pNodes.size();

        double mybad = 0;

        for ( int i=0; i < nodeCount; i++ ){
            Node n = pNodes.get( ( start + i ) % nodeCount );

            if ( ! n.secondary() ){
                mybad++;
                continue;
            } else if (pTagKey != null && !n.checkTag( pTagKey, pTagValue )){
                mybad++;
                continue;
            }

            if ( best == null ){
                best = n;
                badBeforeBest = mybad;
                mybad = 0;
                continue;
            }

            float diff = best._pingTime - n._pingTime;

            // this is a complex way to make sure we get a random distribution of slaves
            if ( diff > slaveAcceptableLatencyMS || ( ( badBeforeBest - mybad ) / ( nodeCount  - 1 ) ) > pRandom.nextDouble() && diff > -1*slaveAcceptableLatencyMS ) {
                best = n;
                badBeforeBest = mybad;
                mybad = 0;
            }

        }

        return ( best != null ) ? best._addr : null;
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

    /**
     * The replica set node object.
     */
    static class Node {

        Node(   ServerAddress addr,
                List<Node> all,
                AtomicReference<Logger> logger,
                Mongo mongo,
                MongoOptions mongoOptions,
                AtomicInteger maxBsonObjectSize,
                AtomicReference<String> setName,
                AtomicReference<String> lastPrimarySignal )
        {
            _addr = addr;
            _all = all;
            _mongoOptions = mongoOptions;
            _port = new DBPort( addr , null , _mongoOptions );
            _names.add( addr.toString() );
            _logger = logger;
            _mongo = mongo;

            _maxBsonObjectSize = maxBsonObjectSize;
            _setName = setName;
            _lastPrimarySignal = lastPrimarySignal;

        }

        private void updateAddr() {
            try {
                if (_addr.updateInetAddr()) {
                    // address changed, need to use new ports
                    _port = new DBPort(_addr, null, _mongoOptions);
                    _mongo.getConnector().updatePortPool(_addr);
                    _logger.get().log(Level.INFO, "Address of host " + _addr.toString() + " changed to " + _addr.getSocketAddress().toString());
                }
            } catch (UnknownHostException ex) {
                _logger.get().log(Level.WARNING, null, ex);
            }
        }

        synchronized void update(){
            update(null);
        }

        synchronized void update(Set<Node> seenNodes){
            try {
                long start = System.currentTimeMillis();
                CommandResult res = _port.runCommand( _mongo.getDB("admin") , _isMasterCmd );
                boolean first = (_lastCheck == 0);
                _lastCheck = System.currentTimeMillis();
                float newPing = _lastCheck - start;
                if (first)
                    _pingTime = newPing;
                else
                    _pingTime = _pingTime + ((newPing - _pingTime) / latencySmoothFactor);
                _rootLogger.log( Level.FINE , "Latency to " + _addr + " actual=" + newPing + " smoothed=" + _pingTime );

                if ( res == null ){
                    throw new MongoInternalException("Invalid null value returned from isMaster");
                }

                if (!_ok) {
                    _logger.get().log( Level.INFO , "Server seen up: " + _addr );
                }
                _ok = true;
                _isMaster = res.getBoolean( "ismaster" , false );
                _isSecondary = res.getBoolean( "secondary" , false );
                _lastPrimarySignal.set( res.getString( "primary" ) );

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
                        _maxBsonObjectSize.set(((Integer)res.get( "maxBsonObjectSize" )).intValue());
                    else
                        _maxBsonObjectSize.set(Bytes.MAX_OBJECT_SIZE);
                }

                if (res.containsField("setName")) {
	                String setName = res.get( "setName" ).toString();
	                if ( _setName == null ){
	                    _setName.set(setName);
	                    _logger.set( Logger.getLogger( _rootLogger.getName() + "." + setName ) );
	                }
	                else if ( !_setName.equals( setName ) ){
	                    _logger.get().log( Level.SEVERE , "mis match set name old: " + _setName.get() + " new: " + setName );
	                    return;
	                }
                }

            }
            catch ( Exception e ){
                if (_ok == true) {
                    _logger.get().log( Level.WARNING , "Server seen down: " + _addr, e );
                } else if (Math.random() < 0.1) {
                    _logger.get().log( Level.WARNING , "Server seen down: " + _addr );
                }
                _ok = false;
            }

            if ( ! _isMaster )
                return;
        }

        Node _addIfNotHere( String host ){
            Node n = findNode( host, _all, _logger );
            if ( n == null ){
                try {
                    n = new Node( new ServerAddress( host ), _all, _logger, _mongo, _mongoOptions, _maxBsonObjectSize, _setName, _lastPrimarySignal );
                    _all.add( n );
                }
                catch ( UnknownHostException un ){
                    _logger.get().log( Level.WARNING , "couldn't resolve host [" + host + "]" );
                }
            }
            return n;
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
            if(_tags != null && _tags.size() > 0)
		        buf.append( "tags:" ).append( JSON.serialize( _tags )  );
            buf.append("}");

            return buf.toString();
        }

        public void close() {
            _port.close();
            _port = null;
        }

        final ServerAddress _addr;
        private final Set<String> _names = Collections.synchronizedSet( new HashSet<String>() );
        private DBPort _port; // we have our own port so we can set different socket options and don't have to owrry about the pool
        private final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>( );

        boolean _ok = false;
        long _lastCheck = 0;
        float _pingTime = 0;

        boolean _isMaster = false;
        boolean _isSecondary = false;

        double _priority = 0;

        private final AtomicReference<Logger> _logger;
        private final MongoOptions _mongoOptions;
        private final Mongo _mongo;
        private final AtomicInteger _maxBsonObjectSize;
        private final AtomicReference<String> _setName;
        private final AtomicReference<String> _lastPrimarySignal;
        private final List<Node> _all;
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
                    _logger.get().log( Level.WARNING , "couldn't do update pass" , e );
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
            n = findNode( _lastPrimarySignal.get() );
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

        if (seenNodes.size() > 0) {
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

    Node findNode( String host ){
        return findNode( host, _all, _logger );
    }

    private static Node findNode( String host, List<Node> all, AtomicReference<Logger> logger ){
        for ( int i=0; i<all.size(); i++ )
            if ( all.get(i)._names.contains( host ) )
                return all.get(i);

        ServerAddress addr = null;
        try {
            addr = new ServerAddress( host );
        }
        catch ( UnknownHostException un ){
            logger.get().log( Level.WARNING , "couldn't resolve host [" + host + "]" );
            return null;
        }

        for ( int i=0; i<all.size(); i++ ){
            if ( all.get(i)._addr.equals( addr ) ){
                all.get(i)._names.add( host );
                return all.get(i);
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
        return _maxBsonObjectSize.get();
    }

    private final List<Node> _all;
    Updater _updater;
    Mongo _mongo;
    private final AtomicReference<String> _setName = new AtomicReference<String>(); // null until init
    private final AtomicInteger _maxBsonObjectSize = new AtomicInteger(0);

    // will get changed to use set name once its found
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(_rootLogger);

    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>();
    boolean _closed = false;

    final Random _random = new Random();
    long _nextResolveTime;

    static int updaterIntervalMS;
    static int slaveAcceptableLatencyMS;
    static int inetAddrCacheMS;
    static float latencySmoothFactor;

    final MongoOptions _mongoOptions;
    static final MongoOptions _mongoOptionsDefaults = new MongoOptions();

    static {
        updaterIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        inetAddrCacheMS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
        latencySmoothFactor = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
        _mongoOptionsDefaults.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        _mongoOptionsDefaults.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
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
