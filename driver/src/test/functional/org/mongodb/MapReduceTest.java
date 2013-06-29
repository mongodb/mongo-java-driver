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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MapReduceTest extends DatabaseTestCase {
    public void setUp() throws Exception {
        super.setUp();
        collection.save(new Document("_id", 0).append("x", new String[]{"a", "b"}));
        collection.save(new Document("_id", 1).append("x", new String[]{"a", "b"}));

        collection.save(new Document("_id", 2).append("x", new String[]{"b", "c"}));
        collection.save(new Document("_id", 3).append("x", new String[]{"c", "d"}));
        collection.save(new Document("_id", 4).append("x", new String[]{"a", "b"}));
        collection.save(new Document("_id", 5).append("x", new String[]{"b", "c"}));
        collection.save(new Document("_id", 6).append("x", new String[]{"b", "c"}));
        collection.save(new Document("_id", 7).append("x", new String[]{"a", "b"}));

        collection.save(new Document("_id", 8).append("x", new String[]{"b", "c"}));
        collection.save(new Document("_id", 9).append("x", new String[]{"c", "d"}));
    }

    @Test
    public void testInlineMapReduce() {
        MongoIterable<Document> results = collection
                .find(new Document("_id", new Document("$gt", 0)))
                .sort(Sort.ascending("_id"))
                .skip(1)
                .limit(6)
                .mapReduce("function(){ for ( var i=0; i < this.x.length; i++ ){ emit( this.x[i] , 1 ); } }",
                        "function(key,values){ var sum=0; for( var i=0; i < values.length; i++ ) sum += values[i]; return sum;}");

        int count = 0;
        for (Document cur : results) {
            System.out.println(cur);
            count++;
        }
        assertEquals(4, count);

        List<Document> all = results.into(new ArrayList<Document>());
        assertEquals(4, all.size());

        List<TagCount> tagCounts = results.map(new Function<Document, TagCount>() {
            @Override
            public TagCount apply(final Document document) {
                return new TagCount(document.getString("_id"), document.getDouble("value"));
            }
        }).into(new ArrayList<TagCount>());
        assertEquals(4, tagCounts.size());
        Collections.sort(tagCounts);
        assertEquals(Arrays.asList(new TagCount("d", 1), new TagCount("a", 2), new TagCount("c", 4), new TagCount("b", 5)), tagCounts);
    }

    static class TagCount implements Comparable<TagCount> {
        private final String tag;
        private final double count;

        TagCount(final String tag, final double count) {
            this.tag = tag;
            this.count = count;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TagCount tagCount = (TagCount) o;

            if (Double.compare(tagCount.count, count) != 0) {
                return false;
            }
            if (!tag.equals(tagCount.tag)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = tag.hashCode();
            temp = Double.doubleToLongBits(count);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        @Override
        public int compareTo(final TagCount o) {
            int cmp = Double.compare(count, o.count);
            if (cmp != 0) {
                return cmp;
            }
            return tag.compareTo(o.tag);
        }

        @Override
        public String toString() {
            return "TagCount{"
                    + "tag='" + tag + '\''
                    + ", count=" + count
                    + '}';
        }
    }
}
