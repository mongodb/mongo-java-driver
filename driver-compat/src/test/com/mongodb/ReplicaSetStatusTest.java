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
import org.mongodb.connection.ServerDescription;

import java.net.UnknownHostException;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ReplicaSetStatusTest {

    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();

    private ClusterDescription clusterDescription;
    private ReplicaSetStatus replicaSetStatus;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        clusterDescription = context.mock(ClusterDescription.class);

        final Cluster cluster = context.mock(Cluster.class);
        context.checking(new Expectations() {{
            allowing(cluster).getDescription();
            will(returnValue(clusterDescription));
        }});

        replicaSetStatus = new ReplicaSetStatus(cluster);
    }

    @Test
    public void shouldReturnReplicaSetName() {
        final String setName = "repl0";

        final ServerDescription serverDescription = context.mock(ServerDescription.class);

        context.checking(new Expectations() {{
            allowing(serverDescription).getSetName();
            will(returnValue(setName));
            allowing(clusterDescription).getAny();
            will(returnValue(singletonList(serverDescription)));
        }});

        assertThat(replicaSetStatus.getName(), is(setName));
    }

    @Test
    public void shouldReturnNullIfNoServers() {
        context.checking(new Expectations() {{
            allowing(clusterDescription).getAny();
            will(returnValue(emptyList()));
        }});

        assertThat(replicaSetStatus.getName(), is(nullValue()));
    }


    @Test
    public void shouldReturnNullIfMasterNotDefined() {
        context.checking(new Expectations() {{
            allowing(clusterDescription).getPrimaries();
            will(returnValue(emptyList()));
        }});

        assertThat(replicaSetStatus.getMaster(), is(nullValue()));
    }

    @Test
    public void shouldReturnMaster() throws UnknownHostException {
        final ServerDescription serverDescription = context.mock(ServerDescription.class);

        context.checking(new Expectations() {{
            allowing(serverDescription).getAddress();
            will(returnValue(new ServerAddress("localhost").toNew()));
            allowing(clusterDescription).getPrimaries();
            will(returnValue(singletonList(serverDescription)));
        }});

        assertThat(replicaSetStatus.getMaster(), is(notNullValue()));
    }

    @Test
    public void shouldTestSpecificServerForBeingMasterOrNot() throws UnknownHostException {
        final ServerDescription primaryDescription = context.mock(ServerDescription.class);
        context.checking(new Expectations() {{
            allowing(primaryDescription).getAddress();
            will(returnValue(new ServerAddress("localhost", 3000).toNew()));
            allowing(clusterDescription).getPrimaries();
            will(returnValue(singletonList(primaryDescription)));
        }});

        assertTrue(replicaSetStatus.isMaster(new ServerAddress("localhost", 3000)));
        assertFalse(replicaSetStatus.isMaster(new ServerAddress("localhost", 4000)));
    }


    @Test
    public void shouldReturnMaxBsonObjectSize() {
        final ServerDescription serverDescription = context.mock(ServerDescription.class);
        context.checking(new Expectations() {{
            allowing(serverDescription).getMaxDocumentSize();
            will(returnValue(47));
            allowing(clusterDescription).getPrimaries();
            will(returnValue(singletonList(serverDescription)));
        }});

        assertThat(replicaSetStatus.getMaxBsonObjectSize(), is(47));
    }

}
