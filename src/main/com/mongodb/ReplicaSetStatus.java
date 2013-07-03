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

import org.bson.util.annotations.Immutable;
import org.bson.util.annotations.ThreadSafe;

import java.net.UnknownHostException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
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
public class ReplicaSetStatus extends ConnectionStatus {

    static final Logger _rootLogger = Logger.getLogger( "com.mongodb.ReplicaSetStatus" );

    ReplicaSetStatus( Mongo mongo, List<ServerAddress> initial ){
        super(initial, mongo);
        _updater = new Updater(initial);
    }

    public String getName() {
        return _replicaSetHolder.get().getSetName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{replSetName: ").append(_replicaSetHolder.get().getSetName());
        sb.append(", members: ").append(_replicaSetHolder);
        sb.append(", updaterIntervalMS: ").append(updaterIntervalMS);
        sb.append(", updaterIntervalNoMasterMS: ").append(updaterIntervalNoMasterMS);
        sb.append(", slaveAcceptableLatencyMS: ").append(slaveAcceptableLatencyMS);
        sb.append(", latencySmoothFactor: ").append(latencySmoothFactor);
        sb.append("}");

        return sb.toString();
    }

    /**
     * @return master or null if don't have one
     * @throws MongoException
     */
    public ServerAddress getMaster(){
        ReplicaSetNode n = getMasterNode();
        if ( n == null )
            return null;
        return n.getServerAddress();
    }

    ReplicaSetNode getMasterNode(){
        checkClosed();
        return _replicaSetHolder.get().getMaster();
    }

    /**
     * @param srv the server to compare
     * @return indication if the ServerAddress is the current Master/Primary
     * @throws MongoException
     */
    public boolean isMaster(ServerAddress srv) {
        if (srv == null)
            return false;

	return srv.equals(getMaster());
    }

    /**
     * @return a good secondary or null if can't find one
     */
    ServerAddress getASecondary() {
        ReplicaSetNode node = _replicaSetHolder.get().getASecondary();
        if (node == null) {
            return null;
        }
        return node._addr;
    }

    @Override
    boolean hasServerUp() {
        for (ReplicaSetNode node : _replicaSetHolder.get().getAll()) {
            if (node.isOk()) {
                return true;
            }
        }
        return false;
    }

    // Simple abstraction over a volatile ReplicaSet reference that starts as null.  The get method blocks until members
    // is not null. The set method notifies all, thus waking up all getters.
    @ThreadSafe
    class ReplicaSetHolder {
        private volatile ReplicaSet members;

        // blocks until replica set is set, or a timeout occurs
        synchronized ReplicaSet get() {
            while (members == null) {
                try {
                    wait(_mongo.getMongoOptions().getConnectTimeout());
                } catch (InterruptedException e) {
                    throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
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
                wait(_mongo.getMongoOptions().getConnectTimeout());
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted while waiting for next update to replica set status", e);
            }
        }

        public synchronized void close() {
            this.members = null;
            notifyAll();
        }

        public String toString() {
            ReplicaSet cur = this.members;
            if (cur != null) {
                return cur.toString();
            }
            return "none";
        }
    }

    // Immutable snapshot state of a replica set. Since the nodes don't change state, this class pre-computes the list
    // of good secondaries so that choosing a random good secondary is dead simple
    @Immutable
    static class ReplicaSet {
        final List<ReplicaSetNode> all;
        final Random random;
        final List<ReplicaSetNode> acceptableSecondaries;
        final List<ReplicaSetNode> acceptableMembers;
        final ReplicaSetNode master;
        final String setName;
        final ReplicaSetErrorStatus errorStatus;

        private int acceptableLatencyMS;
        
        public ReplicaSet(List<ReplicaSetNode> nodeList, Random random, int acceptableLatencyMS) {
            
            this.random = random;
            this.all = Collections.unmodifiableList(new ArrayList<ReplicaSetNode>(nodeList));
            this.acceptableLatencyMS = acceptableLatencyMS;

            errorStatus = validate();
            setName = determineSetName();

            this.acceptableSecondaries =
                    Collections.unmodifiableList(calculateGoodMembers(
                            all, calculateBestPingTime(all, false), acceptableLatencyMS, false));
            this.acceptableMembers =
                    Collections.unmodifiableList(calculateGoodMembers(all, calculateBestPingTime(all, true), acceptableLatencyMS, true));
            master = findMaster();
        }

        public List<ReplicaSetNode> getAll() {
            checkStatus();
            
            return all;
        }

        public boolean hasMaster() {
            return getMaster() != null;
        }

        public ReplicaSetNode getMaster() {
            checkStatus();
            
            return master;
        }

        public int getMaxBsonObjectSize() {
            if (hasMaster()) {
                return getMaster().getMaxBsonObjectSize();
            } else {
                return Bytes.MAX_OBJECT_SIZE;
            }
        }

        public ReplicaSetNode getASecondary() {
            checkStatus();
            
            if (acceptableSecondaries.isEmpty()) {
                return null;
            }
            return acceptableSecondaries.get(random.nextInt(acceptableSecondaries.size()));
        }

        public ReplicaSetNode getASecondary(List<Tag> tags) {
            checkStatus();
            
            // optimization
            if (tags.isEmpty()) {
                return getASecondary();
            }

            List<ReplicaSetNode> acceptableTaggedSecondaries = getGoodSecondariesByTags(tags);

            if (acceptableTaggedSecondaries.isEmpty()) {
                return null;
            }
            return acceptableTaggedSecondaries.get(random.nextInt(acceptableTaggedSecondaries.size()));
        }
        
        public ReplicaSetNode getAMember() {
            checkStatus();
            
            if (acceptableMembers.isEmpty()) {
                return null;
            }
            return acceptableMembers.get(random.nextInt(acceptableMembers.size()));
        }

        public ReplicaSetNode getAMember(List<Tag> tags) {
            checkStatus();
            
            if (tags.isEmpty())
                return getAMember();

            List<ReplicaSetNode> acceptableTaggedMembers = getGoodMembersByTags(tags);

            if (acceptableTaggedMembers.isEmpty())
                return null;
                
            return acceptableTaggedMembers.get(random.nextInt(acceptableTaggedMembers.size()));
        }

        List<ReplicaSetNode> getGoodSecondaries(List<ReplicaSetNode> all) {
            List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(all.size());
            for (ReplicaSetNode cur : all) {
                if (!cur.isOk()) {
                    continue;
                }
                goodSecondaries.add(cur);
            }
            return goodSecondaries;
        }

        public List<ReplicaSetNode> getGoodSecondariesByTags(final List<Tag> tags) {
            checkStatus();
            
            List<ReplicaSetNode> taggedSecondaries = getMembersByTags(all, tags);
            return calculateGoodMembers(taggedSecondaries,
                    calculateBestPingTime(taggedSecondaries, false), acceptableLatencyMS, false);
        }
        
        public List<ReplicaSetNode> getGoodMembersByTags(final List<Tag> tags) {
            checkStatus();
            
            List<ReplicaSetNode> taggedMembers = getMembersByTags(all, tags);
            return calculateGoodMembers(taggedMembers,
                    calculateBestPingTime(taggedMembers, true), acceptableLatencyMS, true);
        }

        public String getSetName() {
            checkStatus();
            
            return setName;
        }
        
        public ReplicaSetErrorStatus getErrorStatus(){
            return errorStatus;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("[ ");
            for (ReplicaSetNode node : getAll())
                sb.append(node.toJSON()).append(",");
            sb.setLength(sb.length() - 1); //remove last comma
            sb.append(" ]");
            return sb.toString();
        }
        
        private void checkStatus(){
            if (!errorStatus.isOk())
                throw new MongoException(errorStatus.getError());
        }

        private ReplicaSetNode findMaster() {
            for (ReplicaSetNode node : all) {
                if (node.master())
                    return node;
            }
            return null;
        }
        
        private String determineSetName() {
            for (ReplicaSetNode node : all) {
                String nodeSetName = node.getSetName();
                
                if (nodeSetName != null && !nodeSetName.equals("")) {
                    return nodeSetName;
                }
            }

            return null;
        }
        
        private ReplicaSetErrorStatus validate() {
            //make sure all nodes have the same set name
            HashSet<String> nodeNames = new HashSet<String>();
            
            for(ReplicaSetNode node : all) {
                String nodeSetName = node.getSetName();
                
                if(nodeSetName != null && !nodeSetName.equals("")) {
                    nodeNames.add(nodeSetName);
                }
            }
            
            if(nodeNames.size() <= 1)
                return new ReplicaSetErrorStatus(true, null);
            else {
                return new ReplicaSetErrorStatus(false, "nodes with different set names detected: " + nodeNames.toString());
            }
        }

        static float calculateBestPingTime(List<ReplicaSetNode> members, boolean includeMaster) {
            float bestPingTime = Float.MAX_VALUE;
            for (ReplicaSetNode cur : members) {
                if (cur.secondary() || (includeMaster && cur.master())) {
                    if (cur._pingTime < bestPingTime) {
                        bestPingTime = cur._pingTime;
                    }
                }
            }
            return bestPingTime;
        }

        static List<ReplicaSetNode> calculateGoodMembers(List<ReplicaSetNode> members, float bestPingTime, int acceptableLatencyMS, boolean includeMaster) {
            List<ReplicaSetNode> goodSecondaries = new ArrayList<ReplicaSetNode>(members.size());
            for (ReplicaSetNode cur : members) {
                if (cur.secondary() || (includeMaster && cur.master())) {
                    if (cur._pingTime - acceptableLatencyMS <= bestPingTime) {
                        goodSecondaries.add(cur);
                    }
                }
            }
            return goodSecondaries;
        }

        static List<ReplicaSetNode> getMembersByTags(List<ReplicaSetNode> members, List<Tag> tags) {
           
            List<ReplicaSetNode> membersByTag = new ArrayList<ReplicaSetNode>();
            
            for (ReplicaSetNode cur : members) {
                if (tags != null && cur.getTags() != null && cur.getTags().containsAll(tags)) {
                    membersByTag.add(cur);
                }
            }

            return membersByTag;
        }

    }

    // Represents the state of a node in the replica set.  Instances of this class are immutable.
    @Immutable
    static class ReplicaSetNode extends Node {
        ReplicaSetNode(ServerAddress addr, Set<String> names, String setName, float pingTime, boolean ok, boolean isMaster, boolean isSecondary,
                       LinkedHashMap<String, String> tags, int maxBsonObjectSize) {
            super(pingTime, addr, maxBsonObjectSize, ok);
            this._names = Collections.unmodifiableSet(new HashSet<String>(names));
            this._setName = setName;
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

        public boolean master(){
            return _ok && _isMaster;
        }

        public boolean secondary(){
            return _ok && _isSecondary;
        }

        public Set<String> getNames() {
            return _names;
        }
        
        public String getSetName() {
            return _setName;
        }

        public Set<Tag> getTags() {
            return _tags;
        }

        public float getPingTime() {
            return _pingTime;
        }

        public String toJSON(){
            StringBuilder buf = new StringBuilder();
            buf.append( "{ address:'" ).append( _addr ).append( "', " );
            buf.append( "ok:" ).append( _ok ).append( ", " );
            buf.append( "ping:" ).append( _pingTime ).append( ", " );
            buf.append( "isMaster:" ).append( _isMaster ).append( ", " );
            buf.append( "isSecondary:" ).append( _isSecondary ).append( ", " );
            buf.append( "setName:" ).append( _setName ).append( ", " );
            buf.append( "maxBsonObjectSize:" ).append( _maxBsonObjectSize ).append( ", " );
            if(_tags != null && _tags.size() > 0){
                List<DBObject> tagObjects = new ArrayList<DBObject>();
                for( Tag tag : _tags)
                    tagObjects.add(tag.toDBObject());
                
                buf.append(new BasicDBObject("tags", tagObjects) );
            }
                
            buf.append("}");

            return buf.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ReplicaSetNode node = (ReplicaSetNode) o;

            if (_isMaster != node._isMaster) return false;
            if (_maxBsonObjectSize != node._maxBsonObjectSize) return false;
            if (_isSecondary != node._isSecondary) return false;
            if (_ok != node._ok) return false;
            if (Float.compare(node._pingTime, _pingTime) != 0) return false;
            if (!_addr.equals(node._addr)) return false;
            if (!_names.equals(node._names)) return false;
            if (!_tags.equals(node._tags)) return false;
            if (!_setName.equals(node._setName)) return false;

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
            result = 31 * result + _setName.hashCode();
            result = 31 * result + _maxBsonObjectSize;
            return result;
        }

        private final Set<String> _names;
        private final Set<Tag> _tags;
        private final boolean _isMaster;
        private final boolean _isSecondary;
        private final String _setName;
    }
    
    
    @Immutable
    static final class ReplicaSetErrorStatus{
        final boolean ok;
        final String error;
        
        ReplicaSetErrorStatus(boolean ok, String error){
            this.ok = ok;
            this.error = error;
        }
        
        public boolean isOk(){
            return ok;
        }
        
        public String getError(){
            return error;
        }
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
        
        public DBObject toDBObject(){
            return new BasicDBObject(key, value);
        }
    }

    // Represents the state of a node in the replica set.  Instances of this class are mutable.
    static class UpdatableReplicaSetNode extends UpdatableNode {

        UpdatableReplicaSetNode(ServerAddress addr,
                                List<UpdatableReplicaSetNode> all,
                                AtomicReference<Logger> logger,
                                Mongo mongo,
                                MongoOptions mongoOptions,
                                AtomicReference<String> lastPrimarySignal) {
            super(addr, mongo, mongoOptions);
            _all = all;
            _names.add(addr.toString());
            _logger = logger;
            _lastPrimarySignal = lastPrimarySignal;
        }

        void update(Set<UpdatableReplicaSetNode> seenNodes) {
            CommandResult res = update();
            if (res == null || !isOk()) {
                return;
            }

            _isMaster = res.getBoolean("ismaster", false);
            _isSecondary = res.getBoolean("secondary", false);
            _lastPrimarySignal.set(res.getString("primary"));

            if (res.containsField("hosts")) {
                for (Object x : (List) res.get("hosts")) {
                    String host = x.toString();
                    UpdatableReplicaSetNode node = _addIfNotHere(host);
                    if (node != null && seenNodes != null)
                        seenNodes.add(node);
                }
            }

            if (res.containsField("passives")) {
                for (Object x : (List) res.get("passives")) {
                    String host = x.toString();
                    UpdatableReplicaSetNode node = _addIfNotHere(host);
                    if (node != null && seenNodes != null)
                        seenNodes.add(node);
                }
            }

            // Tags were added in 2.0 but may not be present
            if (res.containsField("tags")) {
                DBObject tags = (DBObject) res.get("tags");
                for (String key : tags.keySet()) {
                    _tags.put(key, tags.get(key).toString());
                }
            }

            //old versions of mongod don't report setName
            if (res.containsField("setName")) {
                _setName = res.getString("setName", "");
                
                if(_logger.get() == null)
                    _logger.set(Logger.getLogger(_rootLogger.getName() + "." + _setName));
            }
        }

        @Override
        protected Logger getLogger() {
            return _logger.get();
        }

        UpdatableReplicaSetNode _addIfNotHere(String host) {
            UpdatableReplicaSetNode n = findNode(host, _all, _logger);
            if (n == null) {
                try {
                    n = new UpdatableReplicaSetNode(new ServerAddress(host), _all, _logger, _mongo, _mongoOptions, _lastPrimarySignal);
                    _all.add(n);
                } catch (UnknownHostException un) {
                    _logger.get().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                }
            }
            return n;
        }

        private UpdatableReplicaSetNode findNode(String host, List<UpdatableReplicaSetNode> members, AtomicReference<Logger> logger) {
            for (UpdatableReplicaSetNode node : members)
                if (node._names.contains(host))
                    return node;

            ServerAddress addr;
            try {
                addr = new ServerAddress(host);
            } catch (UnknownHostException un) {
                logger.get().log(Level.WARNING, "couldn't resolve host [" + host + "]");
                return null;
            }

            for (UpdatableReplicaSetNode node : members) {
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

        private final Set<String> _names = Collections.synchronizedSet(new HashSet<String>());
        final LinkedHashMap<String, String> _tags = new LinkedHashMap<String, String>();

        boolean _isMaster = false;
        boolean _isSecondary = false;
        String _setName;

        private final AtomicReference<Logger> _logger;
        private final AtomicReference<String> _lastPrimarySignal;
        private final List<UpdatableReplicaSetNode> _all;
    }

    // Thread that monitors the state of the replica set.  This thread is responsible for setting a new ReplicaSet
    // instance on ReplicaSetStatus.members every pass through the members of the set.
    class Updater extends BackgroundUpdater {

        Updater(List<ServerAddress> initial){
            super("ReplicaSetStatus:Updater");
            _all = new ArrayList<UpdatableReplicaSetNode>(initial.size());
            for ( ServerAddress addr : initial ){
                _all.add( new UpdatableReplicaSetNode( addr, _all,  _logger, _mongo, _mongoOptions, _lastPrimarySignal ) );
            }
        }

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    int curUpdateIntervalMS = updaterIntervalNoMasterMS;

                    try {
                        updateAll();

                        ReplicaSet replicaSet = new ReplicaSet(createNodeList(), _random, slaveAcceptableLatencyMS);
                        _replicaSetHolder.set(replicaSet);

                        if (replicaSet.getErrorStatus().isOk() && replicaSet.hasMaster()) {
                            _mongo.getConnector().setMaster(replicaSet.getMaster());
                            curUpdateIntervalMS = updaterIntervalMS;
                        }
                    } catch (Exception e) {
                        _logger.get().log(Level.WARNING, "couldn't do update pass", e);
                    }

                    Thread.sleep(curUpdateIntervalMS);
                }
            }
            catch (InterruptedException e) {
               // Allow thread to exit
            }

            _replicaSetHolder.close();
            closeAllNodes();
        }

        public synchronized void updateAll(){
            HashSet<UpdatableReplicaSetNode> seenNodes = new HashSet<UpdatableReplicaSetNode>();

            for (int i = 0; i < _all.size(); i++) {
                _all.get(i).update(seenNodes);
            }

            if (seenNodes.size() > 0) {
                // not empty, means that at least 1 server gave node list
                // remove unused hosts
                Iterator<UpdatableReplicaSetNode> it = _all.iterator();
                while (it.hasNext()) {
                    if (!seenNodes.contains(it.next()))
                        it.remove();
                }
            }
        }

        private List<ReplicaSetNode> createNodeList() {
            List<ReplicaSetNode> nodeList = new ArrayList<ReplicaSetNode>(_all.size());
            for (UpdatableReplicaSetNode cur : _all) {
                nodeList.add(new ReplicaSetNode(cur._addr, cur._names, cur._setName, cur._pingTimeMS, cur.isOk(), cur._isMaster, cur._isSecondary, cur._tags, cur._maxBsonObjectSize));
            }
            return nodeList;
        }

        private void closeAllNodes() {
            for (UpdatableReplicaSetNode node : _all) {
                try {
                    node.close();
                } catch (final Throwable t) { /* nada */ }
            }
        }

        private final List<UpdatableReplicaSetNode> _all;
        private final Random _random = new Random();
    }

    @Override
    Node ensureMaster() {
        ReplicaSetNode masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode;
        }

        _replicaSetHolder.waitForNextUpdate();

        masterNode = getMasterNode();
        if (masterNode != null) {
            return masterNode;
        }

        return null;
    }

    List<ServerAddress> getServerAddressList() {
        List<ServerAddress> addrs = new ArrayList<ServerAddress>();
        for (ReplicaSetNode node : _replicaSetHolder.get().getAll())
            addrs.add(node.getServerAddress());
        return addrs;
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException
     */
    public int getMaxBsonObjectSize() {
        return _replicaSetHolder.get().getMaxBsonObjectSize();
    }

    final ReplicaSetHolder _replicaSetHolder = new ReplicaSetHolder();

    // will get changed to use set name once its found
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(_rootLogger);

    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>();
    final static int slaveAcceptableLatencyMS;

    static {
        slaveAcceptableLatencyMS = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
    }

}
