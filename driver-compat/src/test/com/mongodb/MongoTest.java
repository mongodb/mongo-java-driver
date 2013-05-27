/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterDescription;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mongodb.connection.ClusterConnectionMode.Direct;
import static org.mongodb.connection.ClusterConnectionMode.Discovering;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ClusterType.Sharded;

public class MongoTest {

    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();

    private ClusterDescription clusterDescription;

    private Mongo mongo;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        clusterDescription = context.mock(ClusterDescription.class);

        final Cluster cluster = context.mock(Cluster.class);
        context.checking(new Expectations() {{
            allowing(cluster).getDescription();
            will(returnValue(clusterDescription));
        }});

        mongo = new Mongo(cluster, MongoClientOptions.builder().build());
    }


    @Test
    public void shouldReturnReplicaSetStatusIfClusterTypeReplicaSetAndModeDiscovering() {
        context.checking(new Expectations() {{
            allowing(clusterDescription).getType();
            will(returnValue(ReplicaSet));
            allowing(clusterDescription).getMode();
            will(returnValue(Discovering));
        }});
        assertThat(mongo.getReplicaSetStatus(), is(notNullValue()));
    }


    @Test
    public void shouldReturnNullIfClusterTypeNotReplica() {
        context.checking(new Expectations() {{
            allowing(clusterDescription).getType();
            will(returnValue(Sharded));
            allowing(clusterDescription).getMode();
            will(returnValue(Discovering));
        }});
        assertThat(mongo.getReplicaSetStatus(), is(nullValue()));
    }

    @Test
    public void shouldReturnNullIfClusterModeNotDiscovering() {
        context.checking(new Expectations() {{
            allowing(clusterDescription).getType();
            will(returnValue(ReplicaSet));
            allowing(clusterDescription).getMode();
            will(returnValue(Direct));
        }});
        assertThat(mongo.getReplicaSetStatus(), is(nullValue()));
    }
}
