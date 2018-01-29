/*
 * Copyright 2008-present MongoDB, Inc.
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
import java.util.List;

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_OTHER;
import static java.util.Arrays.asList;
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
        final TagSet tags1 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "1")));
        final TagSet tags2 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "2")));
        final TagSet tags3 = new TagSet(asList(new Tag("foo", "1"), new Tag("bar", "2"), new Tag("baz", "3")));

        long acceptableLatencyMS = 15;
        long bestRoundTripTime = 50;
        long acceptableRoundTripTime = bestRoundTripTime + (acceptableLatencyMS / 2);
        long unacceptableRoundTripTime = bestRoundTripTime + acceptableLatencyMS + 1;

        primary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27017))
                                   .roundTripTime(acceptableRoundTripTime * 1000000L, NANOSECONDS)
                                   .ok(true)
                                   .type(ServerType.REPLICA_SET_PRIMARY)
                                   .tagSet(tags1)
                                   .maxDocumentSize(FOUR_MEG).build();

        secondary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27018))
                                     .roundTripTime(bestRoundTripTime * 1000000L, NANOSECONDS)
                                     .ok(true)
                                     .type(ServerType.REPLICA_SET_SECONDARY)
                                     .tagSet(tags2)
                                     .maxDocumentSize(FOUR_MEG).build();

        otherSecondary = ServerDescription.builder().state(CONNECTED).address(new ServerAddress(HOST, 27019))
                                          .roundTripTime(unacceptableRoundTripTime * 1000000L, NANOSECONDS)
                                          .ok(true)
                                          .type(ServerType.REPLICA_SET_SECONDARY)
                                          .tagSet(tags3)
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
        setNoPrimary = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(secondary, otherSecondary));
        setNoSecondary = new ClusterDescription(MULTIPLE, REPLICA_SET, asList(primary, uninitiatedMember));
    }


    @Test
    public void testPrimaryReadPreference() {
        assertEquals(1, ReadPreference.primary().choose(set).size());
        assertEquals(primary, ReadPreference.primary().choose(set).get(0));
        assertTrue(ReadPreference.primary().choose(setNoPrimary).isEmpty());
    }

    @Test
    public void testSecondaryReadPreference() {
        TaggableReadPreference pref = (TaggableReadPreference) ReadPreference.secondary();
        List<ServerDescription> candidates = pref.choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        List<TagSet> tagSetList = asList(new TagSet(new Tag("foo", "1")), new TagSet(new Tag("bar", "2")));
        pref = ReadPreference.secondary(tagSetList);
        assertEquals(tagSetList, pref.getTagSetList());

        pref = ReadPreference.secondary(new TagSet(new Tag("baz", "1")));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(new TagSet(new Tag("baz", "2")));
        candidates = pref.choose(set);
        assertEquals(1, candidates.size());
        assertTrue(candidates.contains(secondary));

        pref = ReadPreference.secondary(new TagSet(new Tag("unknown", "1")));
        assertTrue(pref.choose(set).isEmpty());

        pref = ReadPreference.secondary(asList(new TagSet(new Tag("unknown", "1")), new TagSet(new Tag("baz", "2"))));
        candidates = pref.choose(set);
        assertEquals(1, candidates.size());
        assertTrue(candidates.contains(secondary));
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

        pref = ReadPreference.primaryPreferred(new TagSet(new Tag("baz", "2")));
        assertEquals(1, pref.choose(set).size());
        assertEquals(primary, pref.choose(set).get(0));
        assertEquals(1, pref.choose(setNoPrimary).size());
        assertEquals(secondary, pref.choose(setNoPrimary).get(0));
    }

    @Test
    public void testSecondaryPreferredMode() {
        ReadPreference pref = ReadPreference.secondary(new TagSet(new Tag("baz", "2")));
        List<ServerDescription> candidates = pref.choose(set);
        assertEquals(1, candidates.size());
        assertTrue(candidates.contains(secondary));

        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new TagSet(new Tag("unknown", "2")));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.secondaryPreferred();
        candidates = pref.choose(set);
        assertEquals(2, candidates.size());
        assertTrue(candidates.contains(secondary));
        assertTrue(candidates.contains(otherSecondary));

        assertTrue(ReadPreference.secondaryPreferred().choose(setNoSecondary).contains(primary));
    }

    @Test
    public void testNearestMode() {
        ReadPreference pref = ReadPreference.nearest();
        assertTrue(pref.choose(set) != null);

        pref = ReadPreference.nearest(new TagSet(new Tag("baz", "1")));
        assertTrue(pref.choose(set).get(0).equals(primary));

        pref = ReadPreference.nearest(new TagSet(new Tag("baz", "2")));
        assertTrue(pref.choose(set).get(0).equals(secondary));

        pref = ReadPreference.nearest(new TagSet(new Tag("unknown", "2")));
        assertTrue(pref.choose(set).isEmpty());
    }
}
