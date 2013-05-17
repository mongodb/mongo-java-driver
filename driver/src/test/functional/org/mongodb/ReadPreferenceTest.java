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

package org.mongodb;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.connection.ReplicaSetDescription;
import org.mongodb.connection.ReplicaSetMemberDescription;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.Tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceTest {
    private static final int FOUR_MEG = 4 * 1024 * 1024;
    private static final String HOST = "localhost";

    private ReplicaSetMemberDescription primary, secondary, otherSecondary;
    private ReplicaSetDescription set;
    private ReplicaSetDescription setNoSecondary;
    private ReplicaSetDescription setNoPrimary;

    @Before
    public void setUp() throws IOException {
        final Set<Tag> tagSet1 = new HashSet<Tag>();
        tagSet1.add(new Tag("foo", "1"));
        tagSet1.add(new Tag("bar", "2"));
        tagSet1.add(new Tag("baz", "1"));

        final Set<Tag> tagSet2 = new HashSet<Tag>();
        tagSet2.add(new Tag("foo", "1"));
        tagSet2.add(new Tag("bar", "2"));
        tagSet2.add(new Tag("baz", "2"));

        final Set<Tag> tagSet3 = new HashSet<Tag>();
        tagSet3.add(new Tag("foo", "1"));
        tagSet3.add(new Tag("bar", "2"));
        tagSet3.add(new Tag("baz", "3"));

        final float acceptableLatencyMS = 15;
        final float bestPingTime = 50f;
        final float acceptablePingTime = bestPingTime + (acceptableLatencyMS / 2);
        final float unacceptablePingTime = bestPingTime + acceptableLatencyMS + 1;

        primary = new ReplicaSetMemberDescription(new ServerAddress(HOST, 27017),
                ServerDescription.builder().elapsedMillis(acceptablePingTime).ok(true).primary(true).secondary(false).tags(tagSet1)
                        .maxDocumentSize(FOUR_MEG).build(), 0, null);

        secondary = new ReplicaSetMemberDescription(new ServerAddress(HOST, 27018),
                ServerDescription.builder().elapsedMillis(bestPingTime).ok(true).primary(false).secondary(true).tags(tagSet2)
                        .maxDocumentSize(FOUR_MEG).build(), 0, null);

        otherSecondary = new ReplicaSetMemberDescription(new ServerAddress(HOST, 27019),
                ServerDescription.builder().elapsedMillis(unacceptablePingTime).ok(true).primary(false).secondary(true).tags(tagSet3)
                        .maxDocumentSize(FOUR_MEG).build(), 0, null);

        final List<ReplicaSetMemberDescription> nodeList = new ArrayList<ReplicaSetMemberDescription>();
        nodeList.add(primary);
        nodeList.add(secondary);
        nodeList.add(otherSecondary);

        set = new ReplicaSetDescription(nodeList, (new Random()), (int) acceptableLatencyMS);
        setNoPrimary = new ReplicaSetDescription(Arrays.asList(secondary, otherSecondary), (new Random()),
                                      (int) acceptableLatencyMS);
        setNoSecondary = new ReplicaSetDescription(Arrays.asList(primary), (new Random()), (int) acceptableLatencyMS);
    }


    @Test
    public void testStaticPreferences() {
        assertEquals(new Document("mode", "primary"), ReadPreference.primary().toDocument());
        assertEquals(new Document("mode", "secondary"), ReadPreference.secondary().toDocument());
        assertEquals(new Document("mode", "secondaryPreferred"), ReadPreference.secondaryPreferred().toDocument());
        assertEquals(new Document("mode", "primaryPreferred"), ReadPreference.primaryPreferred().toDocument());
        assertEquals(new Document("mode", "nearest"), ReadPreference.nearest().toDocument());
    }

    @Test
    public void testPrimaryReadPreference() {
        assertEquals(primary, ReadPreference.primary().chooseReplicaSetMember(set));
        assertNull(ReadPreference.primary().chooseReplicaSetMember(setNoPrimary));
    }

    @Test
    public void testSecondaryReadPreference() {
        assertTrue(ReadPreference.secondary().toString().startsWith("secondary"));

        ReplicaSetMemberDescription candidate = ReadPreference.secondary().chooseReplicaSetMember(set);
        assertTrue(!candidate.getServerDescription().isPrimary());

        candidate = ReadPreference.secondary().chooseReplicaSetMember(setNoSecondary);
        assertNull(candidate);

        // Test secondary mode, with tags
        ReadPreference pref = ReadPreference.secondary(new Document("foo", "1"), new Document("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));

        candidate = ReadPreference.secondary().chooseReplicaSetMember(set);
        assertTrue((candidate.equals(secondary) || candidate.equals(otherSecondary)) && !candidate.equals(primary));

        pref = ReadPreference.secondary(new Document("baz", "1"));
        assertTrue(pref.chooseReplicaSetMember(set) == null);

        pref = ReadPreference.secondary(new Document("baz", "2"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(secondary));

        pref = ReadPreference.secondary(new Document("madeup", "1"));
        assertEquals(new Document("mode", "secondary").append("tags", Arrays.asList(new Document("madeup", "1"))),
                    pref.toDocument());
        assertTrue(pref.chooseReplicaSetMember(set) == null);
    }

    @Test
    public void testPrimaryPreferredMode() {
        ReadPreference pref = ReadPreference.primaryPreferred();
        final ReplicaSetMemberDescription candidate = pref.chooseReplicaSetMember(set);
        assertEquals(primary, candidate);

        assertNotNull(ReadPreference.primaryPreferred().chooseReplicaSetMember(setNoPrimary));

        pref = ReadPreference.primaryPreferred(new Document("baz", "2"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(primary));
        assertTrue(pref.chooseReplicaSetMember(setNoPrimary).equals(secondary));
    }

    @Test
    public void testSecondaryPreferredMode() {
        ReadPreference pref = ReadPreference.secondary(new Document("baz", "2"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(secondary));

        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new Document("madeup", "1"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(primary));

        pref = ReadPreference.secondaryPreferred();
        final ReplicaSetMemberDescription candidate = pref.chooseReplicaSetMember(set);
        assertTrue((candidate.equals(secondary) || candidate.equals(otherSecondary)) && !candidate.equals(primary));

        assertEquals(primary, ReadPreference.secondaryPreferred().chooseReplicaSetMember(setNoSecondary));
    }

    @Test
    public void testNearestMode() {
        ReadPreference pref = ReadPreference.nearest();
        assertTrue(pref.chooseReplicaSetMember(set) != null);

        pref = ReadPreference.nearest(new Document("baz", "1"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(primary));

        pref = ReadPreference.nearest(new Document("baz", "2"));
        assertTrue(pref.chooseReplicaSetMember(set).equals(secondary));

        pref = ReadPreference.nearest(new Document("madeup", "1"));
        assertEquals(new Document("mode", "nearest").append("tags", Arrays.asList(new Document("madeup", "1"))),
                    pref.toDocument());
        assertTrue(pref.chooseReplicaSetMember(set) == null);
    }

    @Test
    public void testValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));

        final Document first = new Document("dy", "ny");
        final Document remaining = new Document();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining),
                    ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining),
                    ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }

    @Test
    public void testGetName() {
        assertEquals("primary", ReadPreference.primary().getName());
        assertEquals("secondary", ReadPreference.secondary().getName());
        assertEquals("primaryPreferred", ReadPreference.primaryPreferred().getName());
        assertEquals("secondaryPreferred", ReadPreference.secondaryPreferred().getName());
        assertEquals("nearest", ReadPreference.nearest().getName());

        final Document first = new Document("dy", "ny");
        final Document remaining = new Document();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining),
                    ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining),
                    ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }
}