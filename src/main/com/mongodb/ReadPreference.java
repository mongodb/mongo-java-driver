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
        public TaggableReadPreference() { _tags = null;}
        
        public TaggableReadPreference(DBObject ... tagSetList) {
            List<Tag> tagList = new ArrayList<Tag>();
            for (DBObject curTagSet : tagSetList) {
                for (String key : curTagSet.keySet()) {
                    tagList.add(new Tag(key, curTagSet.get(key).toString()));
                }
            }
            _tags = tagList;
        }
        
        public List<Tag> getTags(){
            return _tags;
        }
        
        public List<DBObject> tagsToDBObjectList(){
            List<Tag> taglist = getTags();
            if (taglist == null || taglist.size() <= 0)
                return null;
            
            List<DBObject> tagObjects = new ArrayList<DBObject>();
            for ( Tag tag : taglist )
                tagObjects.add(new BasicDBObject(tag.key, tag.value));
          
            return tagObjects;
        }

        private final List<Tag> _tags;
    }
    
    public static class PrimaryPreferredReadPreference extends SecondaryReadPreference {
        private PrimaryPreferredReadPreference() {}
        public PrimaryPreferredReadPreference( DBObject ... tagSetList) {
            super(tagSetList);
        }
        
        public DBObject toDBObject(){
            DBObject readPrefObject = new BasicDBObject("mode", "primary_preferred");
            
            List<DBObject> tagsList = tagsToDBObjectList();
            if(tagsList != null)
                readPrefObject.put("tags", tagsList);

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
            
            List<DBObject> tagsList = tagsToDBObjectList();
            if(tagsList != null)
                readPrefObject.put("tags", tagsList);

            return readPrefObject;
        }

        @Override
        public String toString(){
           return "ReadPreference.SECONDARY" ;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            Node node = null;
            
            if(getTags() == null || getTags().isEmpty())
                node = set.getASecondary();
            else{
                List<Node> candidates = set.getGoodSecondariesByTags(getTags());
                if( candidates.size() > 0 )
                    node = candidates.get(  (new Random()).nextInt(candidates.size()) );
            }
            return node;
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
            
            List<DBObject> tagsList = tagsToDBObjectList();
            if(tagsList != null)
                readPrefObject.put("tags", tagsList);

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
            
            List<DBObject> tagsList = tagsToDBObjectList();
            if(tagsList != null)
                readPrefObject.put("tags", tagsList);

            return readPrefObject;
        }
        
        @Override
        Node getNode(ReplicaSetStatus.ReplicaSet set) {
            List<Tag> tags = getTags();
            List<Node> candidates = null;
            
            if(tags == null || tags.isEmpty())
                candidates = set.getAll();

            else
                candidates = set.getGoodMembersByTags(tags);
            
            return ( candidates.size() > 0 )? candidates.get(  (new Random()).nextInt(candidates.size()) ) : null;
        }
    }

    @Deprecated
    public static class TaggedReadPreference extends SecondaryPreferredReadPreference {
        public TaggedReadPreference( DBObject tags ) {
            super(splitMapIntoMulitpleMaps(tags));
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
    }
    
    public static ReadPreference PRIMARY = new PrimaryReadPreference();
    public static ReadPreference PRIMARY_PREFERRED = new PrimaryPreferredReadPreference();
    public static ReadPreference SECONDARY = new SecondaryReadPreference();
    public static ReadPreference SECONDARY_PREFERRED = new SecondaryPreferredReadPreference();
    public static ReadPreference NEAREST = new NearestReadPreference();

}
