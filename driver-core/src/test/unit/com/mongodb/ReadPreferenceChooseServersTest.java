/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceChooseServersTest {
    private static final int FOUR_MEG = 4 * 1024 * 1024;
    private static final String HOST = "localhost";

    private ServerDescription primary, secondary, otherSecondary;
    private ClusterDescription set;
    private ClusterDescription setNoSecondary;
    private ClusterDescription setNoPrimary;

    @Before
    public void setUp() throws IOException {
        Tags tags1 = new Tags("foo", "1").append("bar", "2").append("baz", "1");
        Tags tags2 = new Tags("foo", "1").append("bar", "2").append("baz", "2");
        Tags tags3 = new Tags("foo", "1").append("bar", "2").append("baz", "3");

        long acceptableLatencyMS = 15;
        long bestRoundTripTime = 50;
        long acceptableRoundTripTime = bestRoundTripTime + (acceptableLatencyMS / 2);
        long unacceptableRoundTripTime = bestRoundTripTime + acceptableLatencyMS + 1;

        primary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27017))
                                   .roundTripTime(acceptableRoundTripTime * 1000000L, NANOSECONDS)
                                   .ok(true)
                                   .type(ServerType.REPLICA_SET_PRIMARY)
                                   .tags(tags1)
                                   .maxDocumentSize(FOUR_MEG).build();

        secondary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27018))
                                     .roundTripTime(bestRoundTripTime * 1000000L, NANOSECONDS)
                                     .ok(true)
                                     .type(ServerType.REPLICA_SET_SECONDARY)
                                     .tags(tags2)
                                     .maxDocumentSize(FOUR_MEG).build();

        otherSecondary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27019))
                                          .roundTripTime(unacceptableRoundTripTime * 1000000L, NANOSECONDS)
                                          .ok(true)
                                          .type(ServerType.REPLICA_SET_SECONDARY)
                                          .tags(tags3)
                                          .maxDocumentSize(FOUR_MEG)
                                          .build();
        ServerDescription uninitiatedMember = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27020))
                                                               .roundTripTime(unacceptableRoundTripTime * 1000000L, NANOSECONDS)
                                                               .ok(true)
                                                               .type(REPLICA_SET_OTHER)
                                                               .maxDocumentSize(FOUR_MEG)
                                                               .build();

        List<ServerDescription> nodeList = new ArrayList<ServerDescription>();
        nodeList.add(primary);
        nodeList.add(secondary);
        nodeList.add(otherSecondary);
        nodeList.add(uninitiatedMember);

        set = new ClusterDescription(MULTIPLE, REPLICA_SET, nodeList);
        setNoPrimary = new ClusterDescription(MULTIPLE, REPLICA_SET, Arrays.asList(secondary, otherSecondary));
        setNoSecondary = new ClusterDescription(MULTIPLE, REPLICA_SET, Arrays.asList(primary, uninitiatedMember));
    }


    @Test
    public void testPrimaryReadPreference() {
        assertEquals(1, ReadPreference.primary().choose(set).size());
        assertEquals(primary, ReadPreference.primary().choose(set).get(0));
        assertTrue(ReadPreference.primary().choose(setNoPrimary).isEmpty());
    }

    @Test
    public void testSecondaryReadPreference() {
        assertTrue(ReadPreference.secondary().toString().startsWith("secondary"));

        List<ServerDescription> candidates = ReadPreference.secondary().choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        candidates = ReadPreference.secondary().choose(setNoSecondary);
        assertTrue(candidates.isEmpty());

        // Test secondary mode, with tags
        //        List<String> stringList = Arrays.asList("foo", "bar");
        //        List<TagMap> tagsList2 = Arrays.asList(TagMap.singleton("foo", "bar"), TagMap.singleton("bar", "baz"));
        //        List<Map<String, String>> tagsList3 = Arrays.asList(Collections.singletonMap("foo", "1"));
        //        List<Map<String, String>> tagsList4 = Arrays.asList(Collections.<String, String>singletonMap("foo", "1"));

        List<Tags> tagsList = Arrays.asList(new Tags("foo", "1"), new Tags("bar", "2"));
        ReadPreference pref = ReadPreference.secondary(tagsList);
        assertTrue(pref.toString().startsWith("secondary"));

        candidates = ReadPreference.secondary().choose(set);
        assertTrue((candidates.get(0).equals(secondary) || candidates.get(0).equals(otherSecondary)) && !candidates.get(0).equals(primary));

        pref = ReadPreference.secondary(new Tags("baz", "1"));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(new Tags("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.secondary(new Tags("madeup", "1"));
        //        assertEquals(Collections.<String, String>singletonMap("mode", "secondary")
        //                .append("tags", Arrays.asList(Collections.<String, String>singletonMap("madeup", "1"))),
        //                pref.toDocument());
        assertTrue(pref.choose(set).isEmpty());
    }

    @Test
    public void testPrimaryPreferredMode() {
        ReadPreference pref = ReadPreference.primaryPreferred();
        List<ServerDescription> candidates = pref.choose(set);
        assertEquals(1, candidates.size());
        assertEquals(primary, candidates.get(0));

        candidates = pref.choose(setNoPrimary);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        pref = ReadPreference.primaryPreferred(new Tags("baz", "2"));
        assertEquals(1, pref.choose(set).size());
        assertEquals(primary, pref.choose(set).get(0));
        assertEquals(1, pref.choose(setNoPrimary).size());
        assertEquals(secondary, pref.choose(setNoPrimary).get(0));
    }

    @Test
    public void testSecondaryPreferredMode() {
        ReadPreference pref = ReadPreference.secondary(new Tags("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new Tags("madeup", "1"));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.secondaryPreferred();
        List<ServerDescription> candidates = pref.choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        assertTrue(ReadPreference.secondaryPreferred().choose(setNoSecondary).contains(primary));
    }

    @Test
    public void testNearestMode() {
        ReadPreference pref = ReadPreference.nearest();
        assertTrue(pref.choose(set) != null);

        pref = ReadPreference.nearest(new Tags("baz", "1"));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.nearest(new Tags("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.nearest(new Tags("madeup", "1"));
        //        assertEquals(new Tags("mode", "nearest")
        //         .append("tags", Arrays.asList(new Tags("madeup", "1"))),
        //                pref.toDocument());
        assertTrue(pref.choose(set).isEmpty());
    }
}