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
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceServerSelectionTest {
    private static final String HOST = "localhost";

    private ServerDescription primary, secondary, otherSecondary;
    private ClusterDescription set;
    private ClusterDescription setNoSecondary;
    private ClusterDescription setNoPrimary;

    @Before
    public void setUp() throws IOException {
        final TagSet tags1 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "1")));
        final TagSet tags2 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "2")));
        final TagSet tags3 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "3")));

        primary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27017))
                                   .averageLatency(1, SECONDS)
                                   .ok(true)
                                   .type(ServerType.ReplicaSetPrimary)
                                   .tagSet(tags1)
                                   .build();

        secondary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27018))
                                     .averageLatency(1, SECONDS)
                                     .ok(true)
                                     .type(ServerType.ReplicaSetSecondary)
                                     .tagSet(tags2)
                                     .build();

        otherSecondary = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27019))
                                          .averageLatency(1, SECONDS)
                                          .ok(true)
                                          .type(ServerType.ReplicaSetSecondary)
                                          .tagSet(tags3)
                                          .build();
        ServerDescription uninitiatedMember = ServerDescription.builder().state(Connected).address(new ServerAddress(HOST, 27020))
                                                               .averageLatency(1, SECONDS)
                                                               .ok(true)
                                                               .type(ServerType.ReplicaSetOther)
                                                               .build();

        final List<ServerDescription> nodeList = new ArrayList<ServerDescription>();
        nodeList.add(primary);
        nodeList.add(secondary);
        nodeList.add(otherSecondary);
        nodeList.add(uninitiatedMember);

        set = new ClusterDescription(Multiple, ReplicaSet, nodeList);
        setNoPrimary = new ClusterDescription(Multiple, ReplicaSet, asList(secondary, otherSecondary, uninitiatedMember));
        setNoSecondary = new ClusterDescription(Multiple, ReplicaSet, asList(primary, uninitiatedMember));
    }

    @Test
    public void testPrimaryReadPreference() {
        assertEquals(1, ReadPreference.primary().choose(set).size());
        assertEquals(primary, ReadPreference.primary().choose(set).get(0));
        assertTrue(ReadPreference.primary().choose(setNoPrimary).isEmpty());
    }

    @Test
    public void testSecondaryReadPreference() {
        assertEquals("secondary", ReadPreference.secondary().toString());

        List<ServerDescription> candidates = ReadPreference.secondary().choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        candidates = ReadPreference.secondary().choose(setNoSecondary);
        assertTrue(candidates.isEmpty());

        candidates = ReadPreference.secondary().choose(set);
        assertTrue((candidates.get(0).equals(secondary) || candidates.get(0).equals(otherSecondary)) && !candidates.get(0).equals(primary));
    }

    @Test
    public void testSecondaryReadPreferenceWithTags() {
        TaggableReadPreference pref = ReadPreference.secondary(new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));

        List<DBObject> tagSets = Arrays.<DBObject>asList(new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2"));

        List<TagSet> tagSetList = Arrays.asList(new TagSet(new Tag("foo", "1")), new TagSet(new Tag("bar", "2")));

        assertEquals(tagSets, pref.getTagSets());
        assertEquals(tagSetList, pref.getTagSetList());

        pref = ReadPreference.secondary(tagSetList);
        assertEquals(tagSets, pref.getTagSets());
        assertEquals(tagSetList, pref.getTagSetList());


        pref = ReadPreference.secondary(new BasicDBObject("baz", "1"));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.secondary(new BasicDBObject("madeup", "1"));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(asList(new TagSet(new Tag("unknown", "1")), new TagSet(new Tag("baz", "2"))));
        assertTrue(pref.choose(set).get(0).equals(secondary));
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
        assertTrue(pref.choose(set).isEmpty());
    }
}
