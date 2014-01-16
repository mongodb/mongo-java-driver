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


import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.ClusterConnectionMode.Multiple;
import static com.mongodb.ClusterType.ReplicaSet;
import static com.mongodb.ServerConnectionState.Connected;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ReadPreferenceTest {
    private static final int FOUR_MEG = 4 * 1024 * 1024;
    private static final String HOST = "localhost";

    private ServerDescription primary, secondary, otherSecondary;
    private ClusterDescription set;
    private ClusterDescription setNoSecondary;
    private ClusterDescription setNoPrimary;

    @Before
    public void setUp() throws IOException {
        final Tags tags1 = new Tags("foo", "1").append("bar", "2").append("baz", "1");
        final Tags tags2 = new Tags("foo", "1").append("bar", "2").append("baz", "2");
        final Tags tags3 = new Tags("foo", "1").append("bar", "2").append("baz", "3");

        final long acceptableLatencyMS = 15;
        final long bestPingTime = 50;
        final long acceptablePingTime = bestPingTime + (acceptableLatencyMS / 2);
        final long unacceptablePingTime = bestPingTime + acceptableLatencyMS + 1;

        primary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27017))
                                   .averagePingTime(acceptablePingTime * 1000000L, java.util.concurrent.TimeUnit.NANOSECONDS)
                                   .ok(true)
                                   .type(ServerType.ReplicaSetPrimary)
                                   .tags(tags1)
                                   .maxDocumentSize(FOUR_MEG).build();

        secondary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27018))
                                     .averagePingTime(bestPingTime * 1000000L, java.util.concurrent.TimeUnit.NANOSECONDS)
                                     .ok(true)
                                     .type(ServerType.ReplicaSetSecondary)
                                     .tags(tags2)
                                     .maxDocumentSize(FOUR_MEG).build();

        otherSecondary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27019))
                                          .averagePingTime(unacceptablePingTime * 1000000L, java.util.concurrent.TimeUnit.NANOSECONDS)
                                          .ok(true)
                                          .type(ServerType.ReplicaSetSecondary)
                                          .tags(tags3)
                                          .maxDocumentSize(FOUR_MEG)
                                          .build();

        final List<ServerDescription> nodeList = new ArrayList<ServerDescription>();
        nodeList.add(primary);
        nodeList.add(secondary);
        nodeList.add(otherSecondary);

        set = new ClusterDescription(Multiple, ReplicaSet, nodeList);
        setNoPrimary = new ClusterDescription(Multiple, ReplicaSet, Arrays.asList(secondary, otherSecondary));
        setNoSecondary = new ClusterDescription(Multiple, ReplicaSet, Arrays.asList(primary));
    }


    @Test
    public void testStaticPreferences() {
        assertEquals(new BasicDBObject("mode", "primary"), ReadPreference.primary().toDBObject());
        assertEquals(new BasicDBObject("mode", "secondary"), ReadPreference.secondary().toDBObject());
        assertEquals(new BasicDBObject("mode", "secondaryPreferred"), ReadPreference.secondaryPreferred().toDBObject());
        assertEquals(new BasicDBObject("mode", "primaryPreferred"), ReadPreference.primaryPreferred().toDBObject());
        assertEquals(new BasicDBObject("mode", "nearest"), ReadPreference.nearest().toDBObject());
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

        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("foo", "1"),  new BasicDBObject("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));

        candidates = ReadPreference.secondary().choose(set);
        assertTrue((candidates.get(0).equals(secondary) || candidates.get(0).equals(otherSecondary)) && !candidates.get(0).equals(primary));

        pref = ReadPreference.secondary(new BasicDBObject("baz", "1"));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.secondary(new BasicDBObject("madeup", "1"));
        //        assertEquals(Collections.<String, String>singletonMap("mode", "secondary")
        //                .append("tags", Arrays.asList(Collections.<String, String>singletonMap("madeup", "1"))),
        //                pref.toDBObject());
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

        pref = ReadPreference.primaryPreferred(new BasicDBObject("baz", "2"));
        assertEquals(1, pref.choose(set).size());
        assertEquals(primary, pref.choose(set).get(0));
        assertEquals(1, pref.choose(setNoPrimary).size());
        assertEquals(secondary, pref.choose(setNoPrimary).get(0));
    }

    @Test
    public void testSecondaryPreferredMode() {
        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new BasicDBObject("madeup", "1"));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.secondaryPreferred();
        final List<ServerDescription> candidates = pref.choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        assertTrue(ReadPreference.secondaryPreferred().choose(setNoSecondary).contains(primary));
    }

    @Test
    public void testNearestMode() {
        ReadPreference pref = ReadPreference.nearest();
        assertEquals(3, pref.choose(set).size());

        pref = ReadPreference.nearest(new BasicDBObject("baz", "1"));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.nearest(new BasicDBObject("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.nearest(new BasicDBObject("madeup", "1"));
        //        assertEquals(new Tags("mode", "nearest")
        //         .append("tags", Arrays.asList(new Tags("madeup", "1"))),
        //                pref.toDBObject());
        assertTrue(pref.choose(set).isEmpty());
    }

    @Test
    public void testValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));

        final DBObject first = new BasicDBObject("dy", "ny");
        assertEquals(ReadPreference.secondary(first), ReadPreference.valueOf("secondary", first));
        assertEquals(ReadPreference.primaryPreferred(first),
                     ReadPreference.valueOf("primaryPreferred", first));
        assertEquals(ReadPreference.secondaryPreferred(first),
                     ReadPreference.valueOf("secondaryPreferred", first));
        assertEquals(ReadPreference.nearest(first), ReadPreference.valueOf("nearest", first));
    }

    @Test
    public void testGetName() {
        assertEquals("primary", ReadPreference.primary().getName());
        assertEquals("secondary", ReadPreference.secondary().getName());
        assertEquals("primaryPreferred", ReadPreference.primaryPreferred().getName());
        assertEquals("secondaryPreferred", ReadPreference.secondaryPreferred().getName());
        assertEquals("nearest", ReadPreference.nearest().getName());

        final DBObject first = new BasicDBObject("dy", "ny");
        assertEquals(ReadPreference.secondary(first), ReadPreference.valueOf("secondary", first));
        assertEquals(ReadPreference.primaryPreferred(first), ReadPreference.valueOf("primaryPreferred", first));
        assertEquals(ReadPreference.secondaryPreferred(first), ReadPreference.valueOf("secondaryPreferred", first));
        assertEquals(ReadPreference.nearest(first), ReadPreference.valueOf("nearest", first));
    }
}
