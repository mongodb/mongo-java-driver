/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ReadPreferenceTest {
    private static final int FOUR_MEG = 4 * 1024 * 1024;

    private ReplicaSetNode _primary, _secondary, _otherSecondary;
    private ReplicaSet _set;
    private ReplicaSet _setNoSecondary;
    private ReplicaSet _setNoPrimary;

    @Before
    public  void setUp() throws IOException, MongoException {
        Set<String> names = new HashSet<String>();
        names.add("primary");

        LinkedHashMap<String, String> tagSet1 = new LinkedHashMap<String, String>();
        tagSet1.put("foo", "1");
        tagSet1.put("bar", "2");
        tagSet1.put("baz", "1");

        LinkedHashMap<String, String> tagSet2 = new LinkedHashMap<String, String>();
        tagSet2.put("foo", "1");
        tagSet2.put("bar", "2");
        tagSet2.put("baz", "2");

        LinkedHashMap<String, String> tagSet3 = new LinkedHashMap<String, String>();
        tagSet3.put("foo", "1");
        tagSet3.put("bar", "2");
        tagSet3.put("baz", "3");

        float acceptableLatencyMS = 15;
        float bestPingTime = 50f;
        float acceptablePingTime = bestPingTime + (acceptableLatencyMS/2);
        float unacceptablePingTime = bestPingTime + acceptableLatencyMS + 1 ;

        _primary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27017), names, "", acceptablePingTime, true, true,
                                      false, tagSet1, FOUR_MEG );

        names.clear();
        names.add("secondary");
        _secondary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27018), names, "", bestPingTime, true, false,
                                        true, tagSet2, FOUR_MEG );

        names.clear();
        names.add("tertiary");
        _otherSecondary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27019), names, "", unacceptablePingTime,
                                             true, false, true, tagSet3, FOUR_MEG );

        List<ReplicaSetNode> nodeList = new ArrayList<ReplicaSetNode>();
        nodeList.add(_primary);
        nodeList.add(_secondary);
        nodeList.add(_otherSecondary);

        _set  = new ReplicaSet(nodeList, (new Random()), (int)acceptableLatencyMS);
        _setNoPrimary = new ReplicaSet(Arrays.asList(_secondary, _otherSecondary), (new Random()), (int)acceptableLatencyMS);
        _setNoSecondary = new ReplicaSet(Arrays.asList(_primary), (new Random()), (int)acceptableLatencyMS);
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
        assertEquals(_primary, ReadPreference.primary().getNode(_set));
        assertNull(ReadPreference.primary().getNode(_setNoPrimary));
    }

    @Test
    public void testSecondaryReadPreference(){
        assertTrue(ReadPreference.secondary().toString().startsWith("secondary"));

        ReplicaSetNode candidate = ReadPreference.secondary().getNode(_set);
        assertTrue(!candidate.master());

        candidate = ReadPreference.secondary().getNode(_setNoSecondary);
        assertNull(candidate);

        // Test secondary mode, with tags
        ReadPreference pref = ReadPreference.secondary(new Document("foo", "1"), new Document("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));

        candidate  = ReadPreference.secondary().getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_otherSecondary) ) && !candidate.equals(_primary) );

        pref = ReadPreference.secondary(new Document("baz", "1"));
        assertTrue(pref.getNode(_set) == null);

        pref = ReadPreference.secondary(new Document("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));

        pref = ReadPreference.secondary(new Document("madeup", "1"));
        assertEquals(new Document("mode", "secondary").append("tags", Arrays.asList(new Document("madeup", "1"))), pref.toDocument());
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testPrimaryPreferredMode(){
        ReadPreference pref = ReadPreference.primaryPreferred();
        ReplicaSetNode candidate = pref.getNode(_set);
        assertEquals(_primary, candidate);

        assertNotNull(ReadPreference.primaryPreferred().getNode(_setNoPrimary));

        pref = ReadPreference.primaryPreferred(new Document("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_primary));
        assertTrue(pref.getNode(_setNoPrimary).equals(_secondary));
    }

    @Test
    public void testSecondaryPreferredMode(){
        ReadPreference pref = ReadPreference.secondary(new Document("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));

        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new Document("madeup", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));

        pref = ReadPreference.secondaryPreferred();
        ReplicaSetNode candidate = pref.getNode(_set);
        assertTrue((candidate.equals(_secondary) || candidate.equals(_otherSecondary)) && !candidate.equals(_primary));

        assertEquals(_primary, ReadPreference.secondaryPreferred().getNode(_setNoSecondary));
    }

    @Test
    public void testNearestMode(){
        ReadPreference pref = ReadPreference.nearest();
        assertTrue(pref.getNode(_set) != null);

        pref = ReadPreference.nearest(new Document("baz", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));

        pref = ReadPreference.nearest(new Document("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));

        pref = ReadPreference.nearest(new Document("madeup", "1"));
        assertEquals(new Document("mode", "nearest").append("tags", Arrays.asList(new Document("madeup", "1"))),
                     pref.toDocument());
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));

        Document first = new Document("dy", "ny");
        Document remaining = new Document();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }

    @Test
    public void testGetName() {
        assertEquals("primary", ReadPreference.primary().getName());
        assertEquals("secondary", ReadPreference.secondary().getName());
        assertEquals("primaryPreferred", ReadPreference.primaryPreferred().getName());
        assertEquals("secondaryPreferred", ReadPreference.secondaryPreferred().getName());
        assertEquals("nearest", ReadPreference.nearest().getName());

        Document first = new Document("dy", "ny");
        Document remaining = new Document();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }
}