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

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ReadPreferenceGetNodeTest extends TestCase {
    private static final String setName = "test";
    private static final boolean _isMaster = true;
    private static final boolean _isSecondary = true;
    private static final boolean _isOK = true;

    private static final float acceptableLatencyMS = 15;

    private final ReplicaSetStatus.ReplicaSetNode _primary, _secondary1, _secondary2, _secondary3, _recovering1;
    private final ReplicaSetStatus.ReplicaSet _set;
    private final ReplicaSetStatus.ReplicaSet _setNoSecondary;
    private final ReplicaSetStatus.ReplicaSet _setNoPrimary;
    private final ReplicaSetStatus.ReplicaSet _emptySet;

    private Set<ReplicaSetStatus.ReplicaSetNode> expectedNodeSet;
    private Set<ReplicaSetStatus.ReplicaSetNode> nodeSet;

    public ReadPreferenceGetNodeTest() throws IOException, MongoException {
        Set<String> names = new HashSet<String>();

        LinkedHashMap<String, String> tagSetPrimary = new LinkedHashMap<String, String>();
        tagSetPrimary.put("dc", "ny");

        LinkedHashMap<String, String> tagSet = new LinkedHashMap<String, String>();
        tagSet.put("dc", "ny");
        tagSet.put("rack", "1");

        names.clear();
        names.add("primary");
        _primary = new ReplicaSetStatus.ReplicaSetNode(new ServerAddress("127.0.0.1", 27017), names, setName, 50f, _isOK, _isMaster, !_isSecondary, tagSetPrimary, Bytes.MAX_OBJECT_SIZE);

        names.clear();
        names.add("secondary1");
        _secondary1 = new ReplicaSetStatus.ReplicaSetNode(new ServerAddress("127.0.0.1", 27018), names, setName, 60f, _isOK, !_isMaster, _isSecondary, tagSet, Bytes.MAX_OBJECT_SIZE);

        names.clear();
        names.add("secondary2");
        _secondary2 = new ReplicaSetStatus.ReplicaSetNode(new ServerAddress("127.0.0.1", 27019), names, setName, 66f, _isOK, !_isMaster, _isSecondary, tagSet, Bytes.MAX_OBJECT_SIZE);

        names.clear();
        names.add("secondary3");
        _secondary3 = new ReplicaSetStatus.ReplicaSetNode(new ServerAddress("127.0.0.1", 27019), names, setName, 76f, _isOK, !_isMaster, _isSecondary, tagSet, Bytes.MAX_OBJECT_SIZE);

        names.clear();
        names.add("recovering1");
        _recovering1 = new ReplicaSetStatus.ReplicaSetNode(new ServerAddress("127.0.0.1", 27020), names, setName, 10f, _isOK, !_isMaster, !_isSecondary, tagSet, Bytes.MAX_OBJECT_SIZE);

        List<ReplicaSetStatus.ReplicaSetNode> nodeList = new ArrayList<ReplicaSetStatus.ReplicaSetNode>();
        nodeList.add(_primary);
        nodeList.add(_secondary1);
        nodeList.add(_secondary2);
        nodeList.add(_secondary3);
        nodeList.add(_recovering1);

        _set = new ReplicaSetStatus.ReplicaSet(nodeList, new Random(), (int) acceptableLatencyMS);
        _setNoSecondary = new ReplicaSetStatus.ReplicaSet(Arrays.asList(_primary, _recovering1), new Random(), (int) acceptableLatencyMS);
        _setNoPrimary = new ReplicaSetStatus.ReplicaSet(Arrays.asList(_secondary1, _secondary2, _secondary3, _recovering1), new Random(), (int) acceptableLatencyMS);
        _emptySet = new ReplicaSetStatus.ReplicaSet(new ArrayList<ReplicaSetStatus.ReplicaSetNode>(), new Random(), (int) acceptableLatencyMS);
    }

    @BeforeMethod
    public void setUp() {
        expectedNodeSet = new HashSet<ReplicaSetStatus.ReplicaSetNode>();
        nodeSet = new HashSet<ReplicaSetStatus.ReplicaSetNode>();
    }

    @Test
    public void testNearest() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.nearest().getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary, _secondary1));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedNearest() {
        final TaggableReadPreference taggedNearestReadPreference = ReadPreference.nearest(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(taggedNearestReadPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary, _secondary1));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testSecondaryPreferredWithSecondary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.secondaryPreferred().getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testSecondaryPreferredWithNoSecondary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.secondaryPreferred().getNode(_setNoSecondary));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testSecondaryPreferredWithNoPrimaryOrSecondary() {
        assertNull(ReadPreference.secondaryPreferred().getNode(_emptySet));
    }

    @Test
    public void testTaggedSecondaryPreferredWithSecondary() {
        final TaggableReadPreference readPreference = ReadPreference.secondaryPreferred(new BasicDBObject("dc", "ny"));

        for (int i = 0; i < 1000; i++) {
            nodeSet.add(readPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedSecondaryPreferredWithNoSecondary() {
        final TaggableReadPreference readPreference = ReadPreference.secondaryPreferred(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(readPreference.getNode(_setNoSecondary));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedSecondaryPreferredWithNoPrimaryOrSecondary() {
        final TaggableReadPreference readPreference = ReadPreference.secondaryPreferred(new BasicDBObject("dc", "ny"));
        assertNull(readPreference.getNode(_emptySet));
    }

    @Test
    public void testTaggedSecondaryPreferredWithNoSecondaryMatch() {
        final TaggableReadPreference nonMatchingReadPreference =
                ReadPreference.secondaryPreferred(new BasicDBObject("dc", "ca"));

        for (int i = 0; i < 1000; i++) {
            nodeSet.add(nonMatchingReadPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testSecondaryWithSecondary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.secondary().getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testSecondaryWithNoSecondary() {
        assertNull(ReadPreference.secondary().getNode(_setNoSecondary));
    }

    @Test
    public void testTaggedSecondaryWithSecondary() {
        final TaggableReadPreference taggedSecondaryReadPreference = ReadPreference.secondary(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(taggedSecondaryReadPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedSecondaryWithNoSecondary() {
        final TaggableReadPreference taggedSecondaryReadPreference = ReadPreference.secondary(new BasicDBObject("dc", "ny"));
        assertNull(taggedSecondaryReadPreference.getNode(_setNoSecondary));
    }

    @Test
    public void testPrimaryWithPrimary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.primary().getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testPrimaryWithNoPrimary() {
        assertNull(ReadPreference.primary().getNode(_setNoPrimary));
    }

    @Test
    public void testPrimaryPreferredWithPrimary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.primaryPreferred().getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testPrimaryPreferredWithNoPrimary() {
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(ReadPreference.primaryPreferred().getNode(_setNoPrimary));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedPrimaryPreferredWithPrimary() {
        final TaggableReadPreference readPreference = ReadPreference.primaryPreferred(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(readPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_primary));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    public void testTaggedPrimaryPreferredWithNoPrimary() {
        final TaggableReadPreference readPreference = ReadPreference.primaryPreferred(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(readPreference.getNode(_setNoPrimary));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testTaggedPreference() {
        ReadPreference readPreference = new ReadPreference.TaggedReadPreference(new BasicDBObject("dc", "ny"));
        for (int i = 0; i < 1000; i++) {
            nodeSet.add(readPreference.getNode(_set));
        }

        expectedNodeSet.addAll(Arrays.asList(_secondary1, _secondary2));
        assertEquals(expectedNodeSet, nodeSet);
    }
}