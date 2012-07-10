/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.mongodb.ReplicaSetStatus.Node;
import com.mongodb.ReplicaSetStatus.Tag;

public class ReadPreference {

     Node getNode(ReplicaSetStatus.ReplicaSet set) {
        return null;
     }
     
     public DBObject toDBObject() {
         return null;
      }
     
    public static class PrimaryReadPreference extends ReadPreference {
        private PrimaryReadPreference() {}
        
        @Override
        public String toString(){
           return "ReadPreference.PRIMARY" ;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            return set.getMaster();
        }
        
        @Override
        public DBObject toDBObject(){
            return new BasicDBObject("mode", "primary");
        }
    }

    public static abstract class TaggableReadPreference extends ReadPreference {
        public TaggableReadPreference() { _tags = null; }
        
        public TaggableReadPreference(DBObject ... tagSetList) {
            _tags = new ArrayList<DBObject>();
            for (DBObject curTagSet : tagSetList)
                _tags.add(curTagSet);
        }
        
        public List<DBObject> getTagSets(){
            return _tags;
        }
        
        private final List<DBObject> _tags;
    }
    
    public static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        private PrimaryPreferredReadPreference() {}
        public PrimaryPreferredReadPreference( DBObject ... tagSetList) {
            super(tagSetList);
        }
        
        public DBObject toDBObject(){
            DBObject readPrefObject = new BasicDBObject("mode", "primary_preferred");
            
            if(getTagSets() != null)
                readPrefObject.put("tags", getTagSets());

            return readPrefObject;
        }
        
        @Override
        public String toString(){
           return "ReadPreference.PRIMARY_PREFERRED" ;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            Node node = set.getMaster();
            return ( node != null )? node : super.getNode(set);
        }
    }

    public static class SecondaryReadPreference extends TaggableReadPreference {
        private SecondaryReadPreference() {}
        public SecondaryReadPreference(DBObject ... tagSetList){
            super(tagSetList);
        }
        
        @Override
        public DBObject toDBObject(){
            DBObject readPrefObject = new BasicDBObject("mode", "secondary");
            
            if(getTagSets() != null)
                readPrefObject.put("tags", getTagSets());

            return readPrefObject;
        }

        @Override
        public String toString(){
           return "ReadPreference.SECONDARY" ;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            
            if(getTagSets() == null || getTagSets().isEmpty())
                return set.getASecondary();
            
            for (DBObject curTagSet : getTagSets()) {
                List<Tag> tagList = new ArrayList<Tag>();
                for (String key : curTagSet.keySet()) {
                    tagList.add(new Tag(key, curTagSet.get(key).toString()));
                }
                Node node = set.getASecondary(tagList);
                if (node != null) {
                    return node;
                }
            }
            return null;
        }
    }

    public static class SecondaryPreferredReadPreference extends SecondaryReadPreference {
        private SecondaryPreferredReadPreference() {}
        public SecondaryPreferredReadPreference( DBObject ... tagSetList) {
            super(tagSetList);
        }
        
        @Override
        public String toString(){
            return "ReadPreference.SECONDARY_PREFERRED";
        }
        
        public DBObject toDBObject(){
            DBObject readPrefObject = new BasicDBObject("mode", "secondary_preferred");
            
            if(getTagSets() != null)
                readPrefObject.put("tags", getTagSets());

            return readPrefObject;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            Node node = super.getNode(set);
            return ( node != null )? node : set.getMaster();
        }
    }

    public static class NearestReadPreference extends TaggableReadPreference {
        private NearestReadPreference() {}
        public NearestReadPreference( DBObject ... tagSetList) {
            super(tagSetList);
        }

        @Override
        public String toString(){
            return "ReadPreference.NEAREST" ;
        }
    
        @Override
        public DBObject toDBObject(){
            DBObject readPrefObject = new BasicDBObject("mode", "nearest");
            
            if(getTagSets() != null)
                readPrefObject.put("tags", getTagSets());

            return readPrefObject;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            List<Node> candidates = null;
            
            if(getTagSets() == null || getTagSets().isEmpty())
                candidates = set.getGoodMembers();
            
            for (DBObject curTagSet : getTagSets()) {
                List<Tag> tagList = new ArrayList<Tag>();
                for (String key : curTagSet.keySet()) {
                    tagList.add(new Tag(key, curTagSet.get(key).toString()));
                }
                candidates = set.getGoodMembersByTags(tagList);
            }
            return ( candidates != null && candidates.size() > 0 )? candidates.get(  (new Random()).nextInt(candidates.size()) ) : null;
        }
    }

    @Deprecated
    public static class TaggedReadPreference extends ReadPreference {
        
        public TaggedReadPreference( Map<String, String> tags ) {
                _tags = new BasicDBObject(tags);
        }

        public TaggedReadPreference( DBObject tags ) {
            _tags = tags;
        }
        
        private static DBObject[] splitMapIntoMulitpleMaps(DBObject tags){
            DBObject[] tagList = new DBObject[tags.keySet().size()];
            
            if(tags != null){
                int i = 0;
                for ( String key : tags.keySet() ) {
                    tagList[i] = new BasicDBObject(key, tags.get(key).toString());
                    i++;
                }
            }
            return tagList;
        }
        
        public DBObject getTags(){
            return _tags;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            ReadPreference pref = new SecondaryReadPreference(splitMapIntoMulitpleMaps(_tags));
            return pref.getNode(set);
        }
        
        @Override
        public DBObject toDBObject(){
            ReadPreference pref = new SecondaryReadPreference(splitMapIntoMulitpleMaps(_tags));
            return pref.toDBObject();
        }
        
        private final DBObject _tags;
    }
    
    public static ReadPreference PRIMARY = new PrimaryReadPreference();
    public static ReadPreference PRIMARY_PREFERRED = new PrimaryPreferredReadPreference();
    public static ReadPreference SECONDARY = new SecondaryReadPreference();
    public static ReadPreference SECONDARY_PREFERRED = new SecondaryPreferredReadPreference();
    public static ReadPreference NEAREST = new NearestReadPreference();

}
