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

import java.util.Map;

public class ReadPreference {
    public static class PrimaryReadPreference extends ReadPreference {
        private PrimaryReadPreference() {}
        @Override
        public String toString(){
           return "ReadPreference.PRIMARY" ;
        }
    }

    public static class SecondaryReadPreference extends ReadPreference {
        private SecondaryReadPreference() {}
        @Override
        public String toString(){
            return "ReadPreference.SECONDARY";
        }
    }

    public static class TaggedReadPreference extends ReadPreference {
        public TaggedReadPreference( DBObject tags ) {
            _tags = tags;
        }

        public TaggedReadPreference( Map<String, String> tags ) {
            _tags = new BasicDBObject( tags );
        }

        public DBObject getTags(){
            return _tags;
        }

        @Override
        public String toString(){
            return getTags().toString();
        }

        private final DBObject _tags;

    }

    public static ReadPreference PRIMARY = new PrimaryReadPreference();

    public static ReadPreference SECONDARY = new SecondaryReadPreference();

    public static ReadPreference withTags(Map<String, String> tags) {
        return new TaggedReadPreference( tags );
    }

    public static ReadPreference withTags( final DBObject tags ) {
        return new TaggedReadPreference( tags );
    }
}
