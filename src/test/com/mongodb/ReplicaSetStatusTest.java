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

import com.mongodb.ReplicaSetStatus.ReplicaSetNode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.List;


/**
 * This is a placeholder. A node needs to be able to be created outside of ReplicaSetStatus.
 */
public class ReplicaSetStatusTest {
    private Mongo mongoClient;

    @BeforeClass
    public void beforeClass() throws UnknownHostException {
        mongoClient = new MongoClient(new MongoClientURI("mongodb://127.0.0.1:27017,127.0.0.1:27018"));
    }

    @AfterClass
    public void afterClass() {
        mongoClient.close();
    }

    @Test
    public void testClose() throws InterruptedException {
        ReplicaSetStatus replicaSetStatus = new ReplicaSetStatus(mongoClient, mongoClient.getAllAddress());
        replicaSetStatus.start();
        Assert.assertNotNull(replicaSetStatus._replicaSetHolder.get());

        replicaSetStatus.close();

        replicaSetStatus._updater.join(5000);

        Assert.assertTrue(!replicaSetStatus._updater.isAlive());
    }
    
    @Test
    public void testSetNames() throws Exception {
        String replicaSetName = mongoClient.getConnector().getReplicaSetStatus().getName();
        
        List<ReplicaSetNode> nodes = mongoClient.getConnector().getReplicaSetStatus()._replicaSetHolder.get().getAll();
        
        for(ReplicaSetNode node : nodes){
            Assert.assertEquals(replicaSetName, node.getSetName());
        }
        
    }
}

