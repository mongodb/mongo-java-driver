package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ReplicaSetStatusDomainModelTest extends TestCase {

    @Test
    public void testNode() throws UnknownHostException {
        // test constructor
        ServerAddress addr = new ServerAddress("127.0.0.1");
        Set<String> names = new HashSet<String>();
        names.add("1");
        float pingTime = 10;
        boolean ok = true;
        boolean isMaster = true;
        boolean isSecondary = false;
        int maxBsonObjectSize =  Bytes.MAX_OBJECT_SIZE * 4;
        LinkedHashMap<String, String> tags = new LinkedHashMap<String, String>();
        tags.put("foo", "1");
        tags.put("bar", "2");
        ReplicaSetStatus.Node n = new ReplicaSetStatus.Node(addr, names, pingTime, ok, isMaster, isSecondary, tags,
                maxBsonObjectSize);
        assertTrue(n.isOk());
        assertTrue(n.master());
        assertFalse(n.secondary());
        assertEquals(addr, n.getServerAddress());
        assertEquals(names, n.getNames());
        Set<ReplicaSetStatus.Tag> tagSet = new HashSet<ReplicaSetStatus.Tag>();
        tagSet.add(new ReplicaSetStatus.Tag("foo", "1"));
        tagSet.add(new ReplicaSetStatus.Tag("bar", "2"));
        assertEquals(tagSet, n.getTags());
        assertEquals(maxBsonObjectSize, n.getMaxBsonObjectSize());

        // assert that collections are not modifiable
        try {
            n.getTags().clear();
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            n.getNames().clear();
            Assert.fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testReplicaSet() throws Exception {

        List<ReplicaSetStatus.UpdatableNode> updatableNodes = new ArrayList<ReplicaSetStatus.UpdatableNode>();
        List<ReplicaSetStatus.Node> nodes = new ArrayList<ReplicaSetStatus.Node>();

        final Random random = new Random();


        LinkedHashMap<String, String> emptyTagMap = new LinkedHashMap<String, String>();
        LinkedHashMap<String, String> aTag = new LinkedHashMap<String, String>();
        aTag.put("foo", "1");

        LinkedHashMap<String, String> anotherTag = new LinkedHashMap<String, String>();
        anotherTag.put("bar", "2");

        LinkedHashMap<String, String> twoTags = new LinkedHashMap<String, String>();
        twoTags.putAll(aTag);
        twoTags.putAll(anotherTag);

        addNodeToLists("127.0.0.1", false, 10, updatableNodes, nodes, emptyTagMap);
        addNodeToLists("127.0.0.2", true, 30, updatableNodes, nodes, emptyTagMap);
        addNodeToLists("127.0.0.3", true, 30, updatableNodes, nodes, aTag);
        addNodeToLists("127.0.0.4", true, 30, updatableNodes, nodes, anotherTag);
        addNodeToLists("127.0.0.5", true, 10, updatableNodes, nodes, anotherTag);
        addNodeToLists("127.0.0.6", true, 10, updatableNodes, nodes, aTag);
        addNodeToLists("127.0.0.7", true, 10, updatableNodes, nodes, aTag);
        addNodeToLists("127.0.0.8", true, 10, updatableNodes, nodes, twoTags);
        addNodeToLists("127.0.0.9", true, 10, updatableNodes, nodes, twoTags);

        ReplicaSetStatus.ReplicaSet replicaSet = new ReplicaSetStatus.ReplicaSet(nodes, random, 15);
        assertEquals(random, replicaSet.random);
        assertEquals(nodes, replicaSet.all);
        assertEquals(nodes.get(0), replicaSet.master);
        assertTrue(replicaSet.hasMaster());

        // test getting a secondary
        final Map<String, AtomicInteger> counters = new TreeMap<String, AtomicInteger>();
        counters.put("127.0.0.5", new AtomicInteger(0));
        counters.put("127.0.0.6", new AtomicInteger(0));
        counters.put("127.0.0.7", new AtomicInteger(0));
        counters.put("127.0.0.8", new AtomicInteger(0));
        counters.put("127.0.0.9", new AtomicInteger(0));

        for (int idx = 0; idx < 100000; idx++) {
            final ServerAddress addr = replicaSet.getASecondary().getServerAddress();
            assertNotNull(addr);
            counters.get(addr.getHost()).incrementAndGet();
        }
        assertLess(((getHigh(counters) - getLow(counters)) / (double) getHigh(counters)), .03);

        // test getting a secondary by multiple tags
        List<ReplicaSetStatus.Tag> twoTagsList = new ArrayList<ReplicaSetStatus.Tag>();
//        tags.add(new ReplicaSetStatus.Tag("baz", "3"));
        twoTagsList.add(new ReplicaSetStatus.Tag("foo", "1"));
        twoTagsList.add(new ReplicaSetStatus.Tag("bar", "2"));
        ServerAddress address = replicaSet.getASecondary(twoTagsList).getServerAddress();
        List<ReplicaSetStatus.Node> goodSecondariesByTag = replicaSet.getGoodSecondariesByTags(twoTagsList);
        assertEquals(2, goodSecondariesByTag.size());
        assertEquals("127.0.0.8", goodSecondariesByTag.get(0).getServerAddress().getHost());
        assertEquals("127.0.0.9", goodSecondariesByTag.get(1).getServerAddress().getHost());

        // test randomness of getting a secondary
        counters.clear();
        counters.put("127.0.0.6", new AtomicInteger(0));
        counters.put("127.0.0.7", new AtomicInteger(0));
        counters.put("127.0.0.8", new AtomicInteger(0));
        counters.put("127.0.0.9", new AtomicInteger(0));

        List<ReplicaSetStatus.Tag> tags = new ArrayList<ReplicaSetStatus.Tag>();
//        tags.add(new ReplicaSetStatus.Tag("baz", "3"));
        tags.add(new ReplicaSetStatus.Tag("foo", "1"));
//        tags.add(new ReplicaSetStatus.Tag("bar", "2"));
        for (int idx = 0; idx < 100000; idx++) {
            final ServerAddress addr = replicaSet.getASecondary(tags).getServerAddress();
            assertNotNull(addr);
            counters.get(addr.getHost()).incrementAndGet();
        }
        assertLess(((getHigh(counters) - getLow(counters)) / (double) getHigh(counters)), .04);
    }

    private int getLow(Map<String, AtomicInteger> counters) {
        int low = Integer.MAX_VALUE;
        for (final String host : counters.keySet()) {
            int cur = counters.get(host).get();
            if (cur < low) {
                low = cur;
            }
        }
        return low;
    }

    private int getHigh(Map<String, AtomicInteger> counters) {
        int high = 0;
        for (final String host : counters.keySet()) {
            int cur = counters.get(host).get();
            if (cur > high) {
                high = cur;
            }
        }
        return high;
    }

    private void addNodeToLists(String address, boolean isSecondary, float pingTime,
                                List<ReplicaSetStatus.UpdatableNode> updatableNodes, List<ReplicaSetStatus.Node> nodes,
                                LinkedHashMap<String, String> tags)
            throws Exception {

        ServerAddress serverAddress = new ServerAddress(address);
        ReplicaSetStatus.UpdatableNode updatableNode
                = new ReplicaSetStatus.UpdatableNode(serverAddress, updatableNodes, _logger, null, _mongoOptions,
                _setName, _lastPrimarySignal);
        updatableNode._ok = true;
        updatableNode._pingTimeMS = pingTime;
        updatableNode._isSecondary = isSecondary;
        updatableNode._isMaster = !isSecondary;
        updatableNode._tags.putAll(tags);
        updatableNode._maxBsonObjectSize = Bytes.MAX_OBJECT_SIZE;

        updatableNodes.add(updatableNode);

        nodes.add(new ReplicaSetStatus.Node(serverAddress, Collections.singleton(serverAddress.toString()), pingTime,
                true, !isSecondary, isSecondary, tags, Bytes.MAX_OBJECT_SIZE));
    }

    private final MongoOptions _mongoOptions = new MongoOptions();
    private final AtomicReference<String> _setName = new AtomicReference<String>("test");
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(Logger.getLogger("test"));
    private final AtomicInteger _maxBsonObjectSize = new AtomicInteger(Bytes.MAX_OBJECT_SIZE);
    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>("127.0.0.1");
}
