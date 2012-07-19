package com.mongodb;

import com.mongodb.ReplicaSetStatus.Node;
import com.mongodb.ReplicaSetStatus.ReplicaSet;
import com.mongodb.util.TestCase;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
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
        
        _primary = new Node(new ServerAddress("127.0.0.1", 27017), names, acceptablePingTime, _isOK, _isMaster, !_isSecondary, tagSet1, Bytes.MAX_OBJECT_SIZE );
        
        names.clear();
        names.add("secondary");
        _secondary = new Node(new ServerAddress("127.0.0.1", 27018), names, bestPingTime, _isOK, !_isMaster, _isSecondary, tagSet2, Bytes.MAX_OBJECT_SIZE );
        
        names.clear();
        names.add("tertiary");
        _tertiary = new Node(new ServerAddress("127.0.0.1", 27019), names, unacceptablePingTime, _isOK, !_isMaster, _isSecondary, tagSet3, Bytes.MAX_OBJECT_SIZE );
        
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.add(_primary);
        nodeList.add(_secondary);
        nodeList.add(_tertiary);
        
        _set  = new ReplicaSetStatus.ReplicaSet(nodeList, (new Random()), (int)acceptableLatencyMS);
    }

    @Test
    public void testStaticPreferences() {
            
        assertEquals("primary", ReadPreference.PRIMARY.toString());
        assertEquals("secondary", ReadPreference.SECONDARY.toString());

        assertEquals(_primary, ReadPreference.PRIMARY.getNode(_set));
        assertTrue(ReadPreference.PRIMARY.getNode(_set).master());
        assertFalse(ReadPreference.SECONDARY.getNode(_set).master());

        assertEquals("{ \"mode\" : \"primary\"}", ReadPreference.PRIMARY.toDBObject().toString());
        assertEquals("{ \"mode\" : \"secondary\"}", ReadPreference.SECONDARY.toDBObject().toString());
        assertEquals("{ \"mode\" : \"secondaryPreferred\"}", ReadPreference.secondaryPreferred().toDBObject().toString());
        assertEquals("{ \"mode\" : \"primaryPreferred\"}", ReadPreference.primaryPreferred().toDBObject().toString());
        assertEquals("{ \"mode\" : \"nearest\"}", ReadPreference.nearest().toDBObject().toString());
    }
    
    @Test
    public void testPrimaryReadPreference(){
        ReadPreference primaryRP = ReadPreference.PRIMARY;
        assertEquals(_primary, primaryRP.getNode(_set));
    }
    
    @Test
    public void testSecondaryReadPreference(){
        ReadPreference secondaryRP = ReadPreference.secondary();
        assertTrue(secondaryRP.toString().startsWith("secondary"));
        
        Node candidate = secondaryRP.getNode(_set);
        assertTrue(candidate.isOk());
        assertTrue(!candidate.master());
        
        // Test SECONDARY mode, with tags
        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));
        
        candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set) == null);
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        pref = ReadPreference.secondary(new BasicDBObject("madeup", "1"));
        assertTrue(pref.toDBObject().toString().equals("{ \"mode\" : \"secondary\" , \"tags\" : [ { \"madeup\" : \"1\"}]}"));
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testStaticSecondaryMode(){
     // Test static SECONDARY mode. No tags
        Node candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        // Test SECONDARY mode, with tags
        ReadPreference pref = ReadPreference.secondary(new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2"));
        assertTrue(pref.toString().startsWith("secondary"));
        
        candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set) == null);
        
        pref = ReadPreference.secondary(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
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
        assertTrue((candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary));
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
    @SuppressWarnings( "deprecation" )
    public void testTaggedPreference(){
        ReadPreference pref = new ReadPreference.TaggedReadPreference(new BasicDBObject("bar", "2"));
        assertTrue(!pref.getNode(_set).master());
    }
    
    static boolean _isMaster = true;
    static boolean _isSecondary = true;
    static boolean _isOK = true;

    Node _primary, _secondary, _tertiary;
    ReplicaSet _set;
}
