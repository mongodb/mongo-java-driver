package com.mongodb;

import com.mongodb.ConnectionStatus.Node;
import com.mongodb.ReplicaSetStatus.ReplicaSet;
import com.mongodb.ReplicaSetStatus.ReplicaSetNode;
import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class ReadPreferenceTest extends TestCase  {
    public ReadPreferenceTest() throws IOException, MongoException {
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
        
        _primary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27017), names, "", acceptablePingTime, _isOK, _isMaster, !_isSecondary, tagSet1, Bytes.MAX_OBJECT_SIZE );
        
        names.clear();
        names.add("secondary");
        _secondary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27018), names, "", bestPingTime, _isOK, !_isMaster, _isSecondary, tagSet2, Bytes.MAX_OBJECT_SIZE );
        
        names.clear();
        names.add("tertiary");
        _otherSecondary = new ReplicaSetNode(new ServerAddress("127.0.0.1", 27019), names, "", unacceptablePingTime, _isOK, !_isMaster, _isSecondary, tagSet3, Bytes.MAX_OBJECT_SIZE );
        
        List<ReplicaSetNode> nodeList = new ArrayList<ReplicaSetNode>();
        nodeList.add(_primary);
        nodeList.add(_secondary);
        nodeList.add(_otherSecondary);
        
        _set  = new ReplicaSet(nodeList, (new Random()), (int)acceptableLatencyMS);
        _setNoPrimary = new ReplicaSet(Arrays.asList(_secondary, _otherSecondary), (new Random()), (int)acceptableLatencyMS);
        _setNoSecondary = new ReplicaSet(Arrays.asList(_primary), (new Random()), (int)acceptableLatencyMS);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedStaticMembers() {
        assertSame(ReadPreference.primary(), ReadPreference.PRIMARY);
        assertSame(ReadPreference.secondaryPreferred(), ReadPreference.SECONDARY);
    }

    @Test
    public void testStaticPreferences() {
        assertEquals("{ \"mode\" : \"primary\"}", ReadPreference.primary().toDBObject().toString());
        assertEquals("{ \"mode\" : \"secondary\"}", ReadPreference.secondary().toDBObject().toString());
        assertEquals("{ \"mode\" : \"secondaryPreferred\"}", ReadPreference.secondaryPreferred().toDBObject().toString());
        assertEquals("{ \"mode\" : \"primaryPreferred\"}", ReadPreference.primaryPreferred().toDBObject().toString());
        assertEquals("{ \"mode\" : \"nearest\"}", ReadPreference.nearest().toDBObject().toString());
    }
    
    @Test
    public void testPrimaryReadPreference() {
        assertEquals(_primary, ReadPreference.primary().getNode(_set));
        assertNull(ReadPreference.primary().getNode(_setNoPrimary));
        assertEquals("{ \"mode\" : \"primary\"}", ReadPreference.primary().toDBObject().toString());
    }
    
    @Test
    public void testSecondaryReadPreference(){
        assertTrue(ReadPreference.secondary().toString().startsWith("secondary"));
        
        ReplicaSetNode candidate = ReadPreference.secondary().getNode(_set);
        assertTrue(!candidate.master());

        candidate = ReadPreference.secondary().getNode(_setNoSecondary);
        assertNull(candidate);
        
        // Test secondary mode, with tags
        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));
        
        candidate  = ReadPreference.secondary().getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_otherSecondary) ) && !candidate.equals(_primary) );
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set) == null);
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        pref = ReadPreference.secondary(new BasicDBObject("madeup", "1"));
        assertTrue(pref.toDBObject().toString().equals("{ \"mode\" : \"secondary\" , \"tags\" : [ { \"madeup\" : \"1\"}]}"));
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testPrimaryPreferredMode(){
        ReadPreference pref = ReadPreference.primaryPreferred();
        Node candidate = pref.getNode(_set);
        assertEquals(_primary, candidate);

        assertNotNull(ReadPreference.primaryPreferred().getNode(_setNoPrimary));

        pref = ReadPreference.primaryPreferred(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_primary));
        assertTrue(pref.getNode(_setNoPrimary).equals(_secondary));
    }

    @Test
    public void testSecondaryPreferredMode(){
        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        // test that the primary is returned if no secondaries match the tag
        pref = ReadPreference.secondaryPreferred(new BasicDBObject("madeup", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));
        
        pref = ReadPreference.secondaryPreferred();
        Node candidate = pref.getNode(_set);
        assertTrue((candidate.equals(_secondary) || candidate.equals(_otherSecondary)) && !candidate.equals(_primary));

        assertEquals(_primary, ReadPreference.secondaryPreferred().getNode(_setNoSecondary));
    }
    
    @Test
    public void testNearestMode(){
        ReadPreference pref = ReadPreference.nearest();
        assertTrue(pref.getNode(_set) != null);
        
        pref = ReadPreference.nearest(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));
        
        pref = ReadPreference.nearest(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        pref = ReadPreference.nearest(new BasicDBObject("madeup", "1"));
        assertTrue(pref.toDBObject().toString().equals("{ \"mode\" : \"nearest\" , \"tags\" : [ { \"madeup\" : \"1\"}]}"));
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testValueOf() {
        assertEquals(ReadPreference.primary(), ReadPreference.valueOf("primary"));
        assertEquals(ReadPreference.secondary(), ReadPreference.valueOf("secondary"));
        assertEquals(ReadPreference.primaryPreferred(), ReadPreference.valueOf("primaryPreferred"));
        assertEquals(ReadPreference.secondaryPreferred(), ReadPreference.valueOf("secondaryPreferred"));
        assertEquals(ReadPreference.nearest(), ReadPreference.valueOf("nearest"));

        DBObject first = new BasicDBObject("dy", "ny");
        DBObject remaining = new BasicDBObject();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }

    @Test
    public void testGetName() {
        assertEquals("primary", ReadPreference.primary());
        assertEquals("secondary", ReadPreference.secondary());
        assertEquals("primaryPreferred", ReadPreference.primaryPreferred());
        assertEquals("secondaryPreferred", ReadPreference.secondaryPreferred());
        assertEquals("nearest", ReadPreference.nearest());

        DBObject first = new BasicDBObject("dy", "ny");
        DBObject remaining = new BasicDBObject();
        assertEquals(ReadPreference.secondary(first, remaining), ReadPreference.valueOf("secondary", first, remaining));
        assertEquals(ReadPreference.primaryPreferred(first, remaining), ReadPreference.valueOf("primaryPreferred", first, remaining));
        assertEquals(ReadPreference.secondaryPreferred(first, remaining), ReadPreference.valueOf("secondaryPreferred", first, remaining));
        assertEquals(ReadPreference.nearest(first, remaining), ReadPreference.valueOf("nearest", first, remaining));
    }

    @Test
    @SuppressWarnings( "deprecation" )
    public void testTaggedPreference(){
        ReadPreference pref = new ReadPreference.TaggedReadPreference(new BasicDBObject("bar", "2"));
        assertTrue(!pref.getNode(_set).master());
    }
    
    static boolean _isMaster = true;
    static boolean _isSecondary = true;
    static boolean _isOK = true;

    ReplicaSetNode _primary, _secondary, _otherSecondary;
    ReplicaSet _set;
    ReplicaSet _setNoSecondary;
    ReplicaSet _setNoPrimary;
}
