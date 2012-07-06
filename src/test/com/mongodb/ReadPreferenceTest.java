package com.mongodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.testng.annotations.Test;

import com.mongodb.ReadPreference.*;
import com.mongodb.ReplicaSetStatus.Node;
import com.mongodb.ReplicaSetStatus.ReplicaSet;
import com.mongodb.util.TestCase;

public class ReadPreferenceTest extends TestCase  {
    public ReadPreferenceTest() throws IOException, MongoException {
        cleanupMongo = new Mongo(new MongoURI("mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019"));
        
        if (isStandalone(cleanupMongo))
            _standalone = true;
        
        cleanupDB = "com_mongodb_unittest_ReadPreferenceTest";
        
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
        
        if(!_standalone)
            _set  = new ReplicaSetStatus.ReplicaSet(nodeList, (new Random()), (int)acceptableLatencyMS);
        
    }

    @Test
    public void testStaticPreferences() {
        
        if( _standalone )
            return;
        
        assertTrue(ReadPreference.PRIMARY.toString().equals("ReadPreference.PRIMARY"));
        assertTrue(ReadPreference.PRIMARY.toString().equals("ReadPreference.PRIMARY"));
        assertTrue(ReadPreference.PRIMARY.getNode(_set).equals(_primary));
        assertTrue(ReadPreference.PRIMARY.getNode(_set).master());

        assertTrue(ReadPreference.SECONDARY.toString().equals("ReadPreference.SECONDARY"));
        assertTrue(!ReadPreference.SECONDARY.getNode(_set).master());
        
        assertTrue((ReadPreference.PRIMARY).toJSON().equals("{ mode: 'primary' }"));
        assertTrue((ReadPreference.SECONDARY).toJSON().equals("{ mode: 'secondary' }"));
        assertTrue((ReadPreference.SECONDARY_PREFERRED).toJSON().equals("{ mode: 'secondary_preferred' }"));
        assertTrue((ReadPreference.PRIMARY_PREFERRED).toJSON().equals("{ mode: 'primary_preferred' }"));
        assertTrue((ReadPreference.NEAREST).toJSON().equals("{ mode: 'nearest' }"));
    }
    
    @Test
    public void testPrimaryReadPrefernce(){
        if( _standalone )
            return;
        
        ReadPreference primaryRP = new PrimaryReadPreference();
        assertTrue(primaryRP.getNode(_set).equals(_primary));
    }
    
    @Test
    public void testSecondaryReadPrefernce(){
        if( _standalone )
            return;
        
        ReadPreference secondaryRP = new SecondaryReadPreference();
        assertTrue(secondaryRP.toString().equals("ReadPreference.SECONDARY"));
        
        Node candidate = secondaryRP.getNode(_set);
        assertTrue(candidate.isOk());
        assertTrue(!candidate.master());
        
        // Test SECONDARY mode, with tags
        DBObject[] tagArray = { new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2") };
        
        ReadPreference pref = new ReadPreference.SecondaryReadPreference(tagArray);
        assertTrue(pref.toString().equals("ReadPreference.SECONDARY"));
        
        candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        pref = new ReadPreference.SecondaryReadPreference(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set) == null);
        
        pref = new ReadPreference.SecondaryReadPreference(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        pref = new ReadPreference.SecondaryReadPreference(new BasicDBObject("madeup", "1"));
        assertTrue(pref.toJSON().equals("{ mode: 'secondary', tags: [ { 'madeup' : '1' } ] }"));
        assertTrue(pref.getNode(_set) == null);
    }

    @Test
    public void testStaticSecondaryMode(){
        
        if( _standalone )
            return;
        
     // Test static SECONDARY mode. No tags
        Node candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        // Test SECONDARY mode, with tags
        DBObject[] tagArray = { new BasicDBObject("foo", "1"), new BasicDBObject("bar", "2") };
        
        ReadPreference pref = new ReadPreference.SecondaryReadPreference(tagArray);
        assertTrue(pref.toString().equals("ReadPreference.SECONDARY"));
        
        candidate  = ReadPreference.SECONDARY.getNode(_set);
        assertTrue( (candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary) );
        
        pref = new ReadPreference.SecondaryReadPreference(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set) == null);
        
        pref = new ReadPreference.SecondaryReadPreference(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
    }

    @Test
    public void testSecondaryPreferredMode(){
        
        if( _standalone )
            return;
        
        ReadPreference pref = new ReadPreference.SecondaryPreferredReadPreference(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        // test that the primary is returned if no secondaries match the tag
        pref = new ReadPreference.SecondaryPreferredReadPreference(new BasicDBObject("madeup", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));
        
        pref = new ReadPreference.SecondaryPreferredReadPreference();
        Node candidate = pref.getNode(_set);
        assertTrue((candidate.equals(_secondary) || candidate.equals(_tertiary) ) && !candidate.equals(_primary));
    }
    
    @Test
    public void testNearestMode(){
        if( _standalone )
            return;
        
        ReadPreference pref = new ReadPreference.NearestReadPreference();
        assertTrue(pref.getNode(_set) != null);
        
        pref = new ReadPreference.NearestReadPreference(new BasicDBObject("baz", "1"));
        assertTrue(pref.getNode(_set).equals(_primary));
        
        pref = new ReadPreference.NearestReadPreference(new BasicDBObject("baz", "2"));
        assertTrue(pref.getNode(_set).equals(_secondary));
        
        pref = new ReadPreference.NearestReadPreference(new BasicDBObject("madeup", "1"));
        assertTrue(pref.toJSON().equals("{ mode: 'nearest', tags: [ { 'madeup' : '1' } ] }"));
        assertTrue(pref.getNode(_set) == null);
    }
    
    @Test
    public void testTaggedPreference(){
        if( _standalone )
            return;
        
        ReadPreference pref = new ReadPreference.TaggedReadPreference(new BasicDBObject("bar", "2"));
        assertTrue(!pref.getNode(_set).master());
    }
    
    boolean _isMaster = true;
    boolean _isSecondary = true;
    boolean _isOK = true;
    boolean _standalone = false;
    
    Node _primary, _secondary, _tertiary;
    ReplicaSet _set;
}
