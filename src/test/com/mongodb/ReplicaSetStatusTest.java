/**
 * Copyright (C) 2011 10gen Inc.
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

import org.testng.annotations.Test;

import com.mongodb.ReplicaSetStatus.Node;


import com.mongodb.util.TestCase;

import java.util.*;

/**
 * This is a placeholder. A node needs to be able to be created outside of ReplicaSetStatus.
 */
public class ReplicaSetStatusTest extends TestCase {

    @Test
    public void testFindASecondary() throws Exception {

        //final List<Node> nodes = new ArrayList<Node>();

        //final Node node1 = new Node(new ServerAddress("127.0.0.1", 27017));

        /*
        boolean _ok = false;
        long _lastCheck = 0;
        float _pingTime = 0;

        boolean _isMaster = false;
        boolean _isSecondary = false;

        double _priority = 0;



        final Random random = new Random();

        final ServerAddress addr = ReplicaSetStatus.getASecondary( null, null, nodes, random);
        */
    }
}

