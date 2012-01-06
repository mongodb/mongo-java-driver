/**
 * Copyright (C) 2008 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

// Mongo
import com.mongodb.ReplicaSetStatus.Node;
import org.bson.types.*;
import com.mongodb.util.*;

import org.testng.annotations.Test;

// Java
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests to the static secondary load balancing methods.
 */
public class SecondaryLoadBalanceTest extends TestCase {

    @Test(groups = {"basic"})
    public void testSimpleLoadBalance() throws Exception {
        final List<Node> nodes = new ArrayList<Node>();
        nodes.add(initNewNode("127.0.0.1", nodes, true, 0, 0));
        assertEquals(true, (ReplicaSetStatus.getASecondary(null, null, nodes, new Random()) != null));
    }

    @Test(groups = {"basic"})
    public void testEqualLoadBalance() throws Exception {

        final List<Node> nodes = new ArrayList<Node>();

        final HashMap<String, AtomicLong> counters = new HashMap<String, AtomicLong>();

        for (int idx=1; idx <= 10; idx++) {
            final String host = "127.0.0." + idx;
            nodes.add(initNewNode(host, nodes, true, 1, 0));
            counters.put(host, new AtomicLong(0));
        }

        final Random random = new Random();

        for (int idx=0; idx < 100; idx++) {
            final ServerAddress addr = ReplicaSetStatus.getASecondary(null, null, nodes, random);
            assertNotNull(addr);
            counters.get(addr.getHost()).incrementAndGet();
        }

        System.out.println("---- node count: " + nodes.size());

        for (final String host : counters.keySet()) {
            System.out.println(host + " : " + counters.get(host).get());
            assertEquals(true, counters.get(host).get() > 0);
        }
    }

    /**
     * Create a node with secondary load balancing params.
     */
    private Node initNewNode(   final String pAddr,
                                final List<Node> pNodes,
                                final boolean pIsSecondary,
                                final float pPingTime,
                                final double pPriority)
        throws Exception
    {

        final Node node
        = new Node(new ServerAddress(pAddr), pNodes, _logger, null, _mongoOptions, _maxBsonObjectSize, _setName, _lastPrimarySignal);

        node._ok = true;
        node._pingTime = pPingTime;
        node._isSecondary = pIsSecondary;
        node._priority = pPriority;

        return node;
    }

    private final MongoOptions _mongoOptions = new MongoOptions();
    private final AtomicReference<String> _setName = new AtomicReference<String>("test");
    private final AtomicReference<Logger> _logger = new AtomicReference<Logger>(Logger.getLogger("test"));
    private final AtomicInteger _maxBsonObjectSize = new AtomicInteger(Bytes.MAX_OBJECT_SIZE);
    private final AtomicReference<String> _lastPrimarySignal = new AtomicReference<String>("127.0.0.1");


}

