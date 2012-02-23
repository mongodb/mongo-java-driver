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

import com.mongodb.util.JSON;
import org.bson.util.annotations.Immutable;
import org.bson.util.annotations.ThreadSafe;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO:
//  pull config to get
//  priority
//  slave delay

/**
 * Keeps replica set status.  Maintains a background thread to ping all members of the set to keep the status current.
 */
@ThreadSafe
public class ReplicaSetStatus {

	static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );

    ReplicaSetStatus( Mongo mongo, List<ServerAddress> initial ){
        _mongoOptions = _mongoOptionsDefaults.copy();
        _mongoOptions.socketFactory = mongo._options.socketFactory;
        _mongo = mongo;
        _updater = new Updater(initial);
    }

    void start() {
        _updater.start();
    }

    public String getName() {
        return _setName.get();
    }

    @Override
	public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{replSetName: ").append(_setName.get());
        sb.append(", nextResolveTime: ").append(new Date(_updater.getNextResolveTime()).toString());
        sb.append(", all : ").append(members);

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
        return n.getServerAddress();
    }

    Node getMasterNode(){
        _checkClosed();
        return members.get().getMaster();
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
     * @param tags tags map
     * @return a good secondary by tag value or null if can't find one
     */
    ServerAddress getASecondary( DBObject tags ) {
        // store the reference in local, so that it doesn't change out from under us while looping
        List<Tag> tagList = new ArrayList<Tag>();
        for ( String key : tags.keySet() ) {
            tagList.add(new Tag(key, tags.get(key).toString()));
        }
        Node node =  members.get().getASecondary(tagList);
        if (node != null) {
            return node.getServerAddress();
        }
        return null;
    }

    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary() {
        Node node = members.get().getASecondary();
        if (node == null) {
            return null;
        }
        return node._addr;
    }

    boolean hasServerUp() {
        for (Node node : members.get().getAll()) {
            if (node.isOk()) {
                return true;
            }
        }
        return false;
    }
    
    // Simple abstraction over a volatile ReplicaSet reference that starts as null.  The get method blocks until members
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    static class ReplicaSetHolder {
       private volatile ReplicaSet members;

       // blocks until replica set is set.  It's not necessary to synchronize this method because members is volatile
       synchronized ReplicaSet get() {
           while (members == null) {
               try {
                   wait();
               }
               catch (InterruptedException e) {
                   throw new MongoException("Interrupted while waiting for next update to replica set status", e);
               }
           }
           return members;
       }

       // set the replica set to a non-null value and notifies all threads waiting.  
       synchronized void set(ReplicaSet members) {
           if (members == null) {
               throw new IllegalArgumentException("members can not be null");
           }
           this.members = members;
           notifyAll();
       }

       // blocks until the replica set is set again
       synchronized void waitForNextUpdate() {
           try {
               wait();
           } 
           catch (InterruptedException e) {
              throw new MongoException("Interrupted while waiting for next update to replica set status", e);
           }
       }
    }

    // Immutable snapshot state of a replica set. Since the nodes don't change state, this class pre-computes the list
    // of good secondaries so that choosing a random good secondary is dead simple
    @Immutable
    static class ReplicaSet {
        final List<Node> all;
        final Random random;
        final List<Node> goodSecondaries;
        final Map<Tag, List<Node>> goodSecondariesByTagMap;

        public ReplicaSet(List<UpdatableNode> pMembers, Random random, int acceptableLatencyMS) {
            this.random = random;
            List<Node> all = new ArrayList<Node>(pMembers.size());
            for (UpdatableNode cur : pMembers) {
                all.add(new Node(cur._addr, cur._names, cur._pingTime, cur._ok, cur._isMaster, cur._isSecondary, cur._tags));
            }
            this.all = Collections.unmodifiableList(all);
            this.goodSecondaries =
                    Collections.unmodifiableList(calculateGoodSecondaries(all, calculateBestPingTime(all), acceptableLatencyMS));
            Set<Tag> uniqueTags = new HashSet<Tag>();
            for (Node curNode : all) {
                for (Tag curTag : curNode._tags) {
                    uniqueTags.add(curTag);
                }
            }
            Map<Tag, List<Node>> goodSecondariesByTagMap = new HashMap<Tag, List<Node>>();
            for (Tag curTag : uniqueTags) {
                List<Node> taggedMembers = getMembersByTag(all, curTag);
                goodSecondariesByTagMap.put(curTag,
                        Collections.unmodifiableList(calculateGoodSecondaries(taggedMembers,
                                calculateBestPingTime(taggedMembers), acceptableLatencyMS)));
            }
            this.goodSecondariesByTagMap = Collections.unmodifiableMap(goodSecondariesByTagMap);
            
        }

        public List<Node> getAll() {
            return all;
        }
        
        public Node getMaster() {
            for (Node node : all) {
                if (node.master())
                    return node;
            }
            return null;
        }

        public Node getASecondary() {
            if (goodSecondaries.isEmpty()) {
                return null;
            }
            return goodSecondaries.get(random.nextInt(goodSecondaries.size()));
        }

        public Node getASecondary(List<Tag> tags) {
            for (Tag tag : tags) {
                List<Node> goodSecondariesByTag = goodSecondariesByTagMap.get(tag);
                if (goodSecondariesByTag != null) {
                    Node node = goodSecondariesByTag.get(random.nextInt(goodSecondariesByTag.size()));
                    if (node != null) {
                        return node;
                    }
                }
            }
            return null;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (Node node : getAll())
                sb.append(node.toJSON()).append(",");
            sb.setLength(sb.length() - 1); //remove last comma
            sb.append(" ]");
            return sb.toString();
        }

        static float calculateBestPingTime(List<Node> members) {
            float bestPingTime = Float.MAX_VALUE;
            for (Node cur : members) {
                if (!cur.secondary()) {
                    continue;
                }
                if (cur._pingTime < bestPingTime) {
                    bestPingTime = cur._pingTime;
                }
            }
            return bestPingTime;
        }

        static List<Node> calculateGoodSecondaries(List<Node> members, float bestPingTime, int acceptableLatencyMS) {
            List<Node> goodSecondaries = new ArrayList<Node>(members.size());
            for (Node cur : members) {
                if (!cur.secondary()) {
                    continue;
                }
                if (cur._pingTime - acceptableLatencyMS <= bestPingTime ) {
                    goodSecondaries.add(cur);
                }
            }
            return goodSecondaries;
        }

        static List<Node> getMembersByTag(List<Node> members, Tag tag) {
            List<Node> membersByTag = new ArrayList<Node>();

            for (Node cur : members) {
                if (cur._tags.contains(tag)) {
                    membersByTag.add(cur);
                }
            }

            return membersByTag;
        }
    }

    // Represents the state of a node in the replica set.  Instances of this class are immutable.
    @Immutable
    static class Node {
        Node(ServerAddress addr, Set<String> names, float pingTime, boolean ok, boolean isMaster, boolean isSecondary,
             LinkedHashMap<String, String> tags) {
            this._addr = addr;
            this._names = Collections.unmodifiableSet(new HashSet<String>(names));
            this._pingTime = pingTime;
            this._ok = ok;
            this._isMaster = isMaster;
            this._isSecondary = isSecondary;
            this._tags = Collections.unmodifiableSet(getTagsFromMap(tags));
        }

        private static Set<Tag> getTagsFromMap(LinkedHashMap<String,String> tagMap) {
            Set<Tag> tagSet = new HashSet<Tag>();
            for (Map.Entry<String, String> curEntry : tagMap.entrySet()) {
                tagSet.add(new Tag(curEntry.getKey(), curEntry.getValue()));
            }
            return tagSet;
        }

        public boolean isOk() {
            return _ok;
        }

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
        }

        public ServerAddress getServerAddress() {
            return _addr;
        }

        public Set<String> getNames() {
            return _names;
        }
        
        public Set<Tag> getTags() {
            return _tags;
        }

        public String toJSON(){
            StringBuilder buf = new StringBuilder();
            buf.append( "{ address:'" ).append( _addr ).append( "', " );
            buf.append( "ok:" ).append( _ok ).append( ", " );
            buf.append( "ping:" ).append( _pingTime ).append( ", " );
            buf.append( "isMaster:" ).append( _isMaster ).append( ", " );
            buf.append( "isSecondary:" ).append( _isSecondary ).append( ", " );
            if(_tags != null && _tags.size() > 0)
                buf.append( "tags:" ).append( JSON.serialize( _tags )  );
            buf.append("}");

            return buf.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            if (_isMaster != node._isMaster) return false;
            if (_isSecondary != node._isSecondary) return false;
            if (_ok != node._ok) return false;
            if (Float.compare(node._pingTime, _pingTime) != 0) return false;
            if (!_addr.equals(node._addr)) return false;
            if (!_names.equals(node._names)) return false;
            if (!_tags.equals(node._tags)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = _addr.hashCode();
            result = 31 * result + (_pingTime != +0.0f ? Float.floatToIntBits(_pingTime) : 0);
            result = 31 * result + _names.hashCode();
            result = 31 * result + _tags.hashCode();
            result = 31 * result + (_ok ? 1 : 0);
            result = 31 * result + (_isMaster ? 1 : 0);
            result = 31 * result + (_isSecondary ? 1 : 0);
            return result;
        }

        private final ServerAddress _addr;
        private final float _pingTime;
        private final Set<String> _names;
        private final Set<Tag> _tags;
        private final boolean _ok;
        private final boolean _isMaster;
        private final boolean _isSecondary;
    }

    // Simple class to hold a single tag, both key and value
    @Immutable
    static final class Tag {
        final String key;
        final String value;

        Tag(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Tag tag = (Tag) o;

            if (key != null ? !key.equals(tag.key) : tag.key != null) return false;
            if (value != null ? !value.equals(tag.value) : tag.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }

    // Represents the state of a node in the replica set.  Instances of this class are mutable.
    static class UpdatableNode {

        UpdatableNode(ServerAddress addr,
                      List<UpdatableNode> all,
                      AtomicReference<Logger> logger,
                      Mongo mongo,
                      MongoOptions mongoOptions,
                      AtomicInteger maxBsonObjectSize,
                      AtomicReference<String> setName,
                      AtomicReference<String> lastPrimarySignal)
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

        synchronized void update(Set<UpdatableNode> seenNodes){
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
                        UpdatableNode node = _addIfNotHere(host);
                        if (node != null && seenNodes != null)
                            seenNodes.add(node);
                    }
                }

                if ( res.containsField( "passives" ) ){
                    for ( Object x : (List)res.get("passives") ){
                        String host = x.toString();
                        UpdatableNode node = _addIfNotHere(host);
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

                if ( _isMaster ) {
                    // max size was added in 1.8
                    if (res.containsField("maxBsonObjectSize"))
                        _maxBsonObjectSize.set((Integer) res.get("maxBsonObjectSize"));
                    else
                        _maxBsonObjectSize.set(Bytes.MAX_OBJECT_SIZE);
                }

                if (res.containsField("setName")) {
	                String setName = res.get( "setName" ).toString();
	                if ( _setName.get() == null ){
	                    _setName.set(setName);
	                    _logger.set( Logger.getLogger( _rootLogger.getName() + "." + setName));
	                }
	                else if ( !_setName.get().equals( setName ) ){
	                    _logger.get().log( Level.SEVERE , "mismatch set name old: " + _setName.get() + " new: " + setName );
                    }
                }

            }
            catch ( Exception e ){
                if (_ok) {
                    _logger.get().log( Level.WARNING , "Server seen down: " + _addr, e );
                } else if (Math.random() < 0.1) {
                    _logger.get().log( Level.WARNING , "Server seen down: " + _addr, e );
                }
                _ok = false;
            }
        }

        UpdatableNode _addIfNotHere( String host ){
            UpdatableNode n = findNode( host, _all, _logger );
            if ( n == null ){
                try {
                    n = new UpdatableNode( new ServerAddress( host ), _all, _logger, _mongo, _mongoOptions, _maxBsonObjectSize, _setName, _lastPrimarySignal );
                    _all.add( n );
                }
                catch ( UnknownHostException un ){
                    _logger.get().log( Level.WARNING , "couldn't resolve host [" + host + "]" );
                }
            }
            return n;
        }

        private UpdatableNode findNode( String host, List<UpdatableNode> members, AtomicReference<Logger> logger ){
            for (UpdatableNode node : members)
                if (node._names.contains(host))
                    return node;

            ServerAddress addr;
            try {
                addr = new ServerAddress( host );
            }
            catch ( UnknownHostException un ){
                logger.get().log( Level.WARNING , "couldn't resolve host [" + host + "]" );
                return null;
            }

            for (UpdatableNode node : members) {
                if (node._addr.equals(addr)) {
                    node._names.add(host);
                    return node;
                }
            }

            return null;
        }

        public void close() {
            _port.close();
            _port = null;
        }

        final ServerAddress _addr;
        private final Set<String> _names = Collections.synchronizedSet( new HashSet<String>() );
        private DBPort _port; // we have our own port so we can set different socket options and don't have to owrry about the pool
        final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>( );

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
        private final List<UpdatableNode> _all;
    }

    // Thread that monitors the state of the replica set.  This thread is responsible for setting a new ReplicaSet
    // instance on ReplicaSetStatus.members every pass through the members of the set.
    class Updater extends Thread {

        Updater(List<ServerAddress> initial){
            super( "ReplicaSetStatus:Updater" );
            setDaemon( true );
            _all = new ArrayList<UpdatableNode>(initial.size());
            for ( ServerAddress addr : initial ){
                _all.add( new UpdatableNode( addr, _all,  _logger, _mongo, _mongoOptions, _maxBsonObjectSize, _setName, _lastPrimarySignal ) );
            }
            _nextResolveTime = System.currentTimeMillis() + inetAddrCacheMS;
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    try {
                        updateAll();

                        updateInetAddresses();

                        members.set(new ReplicaSet(_all, _random, slaveAcceptableLatencyMS));
                        // force check on master
                        // otherwise master change may go unnoticed for a while if no write concern
                        _mongo.getConnector().checkMaster(true, false);
                    } catch (Exception e) {
                        _logger.get().log(Level.WARNING, "couldn't do update pass", e);
                    }

                    Thread.sleep(updaterIntervalMS);
                }
            }
            catch (InterruptedException e) {
               // Allow thread to exit
            }

            closeAllNodes();
        }

        public long getNextResolveTime() {
            return _nextResolveTime;
        }

        public synchronized void updateAll(){
            HashSet<UpdatableNode> seenNodes = new HashSet<UpdatableNode>();

            // make a copy of _all, since UpdatableNode.update can add to it
            for (UpdatableNode node : new ArrayList<UpdatableNode>(_all)) {
                node.update(seenNodes);
            }

            if (seenNodes.size() > 0) {
                // not empty, means that at least 1 server gave node list
                // remove unused hosts
                Iterator<UpdatableNode> it = _all.iterator();
                while (it.hasNext()) {
                    if (!seenNodes.contains(it.next()))
                        it.remove();
                }
            }
        }

        private void updateInetAddresses() {
            long now = System.currentTimeMillis();
            if (inetAddrCacheMS > 0 && _nextResolveTime < now) {
                _nextResolveTime = now + inetAddrCacheMS;
                for (UpdatableNode node : _all) {
                    node.updateAddr();
                }
            }
        }

        private void closeAllNodes() {
            for (UpdatableNode node : _all) {
                try {
                    node.close();
                } catch (final Throwable t) { /* nada */ }
            }
        }

        private final List<UpdatableNode> _all;
        private volatile long _nextResolveTime;
        private final Random _random = new Random();
    }

    /**
     * Ensures that we have the current master, if there is one. If the current snapshot of the replica set
     *
     * @return address of the current master, or null if there is none
     */
    ServerAddress ensureMaster(){
        Node masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode.getServerAddress();
        }

        members.waitForNextUpdate();

        masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode.getServerAddress();
        }

        return null;
    }

    List<ServerAddress> getServerAddressList() {
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        for (Node node : members.get().getAll())
            addrs.add(node.getServerAddress());
        return addrs;
    }

    void close() {
        _closed = true;
        _updater.interrupt();
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     */
    public int getMaxBsonObjectSize() {
        return _maxBsonObjectSize.get();
    }

    final ReplicaSetHolder members = new ReplicaSetHolder();

    final Updater _updater;
    private final Mongo _mongo;
    private final AtomicReference<String> _setName = new AtomicReference<String>(); // null until init
    private final AtomicInteger _maxBsonObjectSize = new AtomicInteger(0);

    // will get changed to use set name once its found
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(_rootLogger);

    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>();
    private volatile boolean _closed;

    final static int updaterIntervalMS;
    final static int slaveAcceptableLatencyMS;
    final static int inetAddrCacheMS;
    final static float latencySmoothFactor;

    private final MongoOptions _mongoOptions;
    private static final MongoOptions _mongoOptionsDefaults = new MongoOptions();

    static {
        updaterIntervalMS = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        inetAddrCacheMS = Integer.parseInt(System.getProperty("com.mongodb.inetAddrCacheMS", "300000"));
        latencySmoothFactor = Float.parseFloat(System.getProperty("com.mongodb.latencySmoothFactor", "4"));
        _mongoOptionsDefaults.connectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        _mongoOptionsDefaults.socketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
    }

    static final DBObject _isMasterCmd = new BasicDBObject( "ismaster" , 1 );
}
