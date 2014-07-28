/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb.acceptancetest.querying;

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoIterable;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.Function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mongodb.Sort.ascending;

public class MapReduceAcceptanceTest extends DatabaseTestCase {
    private void insertLabelData() {
        collection.save(new Document("_id", 0).append("labels", asList("a", "b")));
        collection.save(new Document("_id", 1).append("labels", asList("a", "b")));

        collection.save(new Document("_id", 2).append("labels", asList("b", "c")));
        collection.save(new Document("_id", 3).append("labels", asList("c", "d")));
        collection.save(new Document("_id", 4).append("labels", asList("a", "b")));
        collection.save(new Document("_id", 5).append("labels", asList("b", "c")));
        collection.save(new Document("_id", 6).append("labels", asList("b", "c")));
        collection.save(new Document("_id", 7).append("labels", asList("a", "b")));

        collection.save(new Document("_id", 8).append("labels", asList("b", "c")));
        collection.save(new Document("_id", 9).append("labels", asList("c", "d")));
    }

    @Test
    public void shouldReturnCountOfAllArrayValuesUsingSimpleMapReduce() {
        //given
        insertLabelData();

        //when
        // perform Map Reduce on all data
        MongoIterable<Document> results = collection.find()
                                                    .mapReduce("  function(){ "
                                                               + "  for ( var i=0; i < this.labels.length; i++ ){ "
                                                               + "    emit( this.labels[i] , 1 ); "
                                                               + "  }"
                                                               + "}",

                                                               "  function(key,values){ "
                                                               + "  var sum=0; "
                                                               + "  for( var i=0; i < values.length; i++ ) "
                                                               + "    sum += values[i]; "
                                                               + "  return sum;"
                                                               + "}");

        //then
        List<Document> resultList = results.into(new ArrayList<Document>());
        assertThat("There are four distinct labels, a b c d", resultList.size(), is(4));

        assertThat("There are four 'a's in the data", resultList, hasItem(new Document("_id", "a").append("value", 4.0)));
        assertThat("There are eight 'b's in the data", resultList, hasItem(new Document("_id", "b").append("value", 8.0)));
        assertThat("There are six 'c's in the data", resultList, hasItem(new Document("_id", "c").append("value", 6.0)));
        assertThat("There are two 'd's in the data", resultList, hasItem(new Document("_id", "d").append("value", 2.0)));
    }

    @Test
    public void shouldPerformMapReduceOnALimitedSetOfData() {
        //given
        insertLabelData();

        //when
        MongoIterable<Document> results = collection.find(new Document("_id", new Document("$gt", 0))) //find all IDs greater than zero
                                              .sort(ascending("_id"))   // sort by ID
                                              .skip(1)                  // skip the first in the results
                                              .limit(6)                 // limit to 6 of the remaining results
                                              .mapReduce("  function(){ "
                                                         + "  for ( var i=0; i < this.labels.length; i++ ){ "
                                                         + "    emit( this.labels[i] , 1 ); "
                                                         + "  }"
                                                         + "}",

                                                         "  function(key,values){ "
                                                         + "  var sum=0; "
                                                         + "  for( var i=0; i < values.length; i++ ) "
                                                         + "    sum += values[i]; "
                                                         + "  return sum;"
                                                         + "}");
        // will perform Map Reduce on _ids 2-7

        //then
        List<Document> resultList = results.into(new ArrayList<Document>());
        assertThat("There are four distinct labels, a b c d", resultList.size(), is(4));

        assertThat("There are two 'a's in the data", resultList, hasItem(new Document("_id", "a").append("value", 2.0)));
        assertThat("There are five 'b's in the data", resultList, hasItem(new Document("_id", "b").append("value", 5.0)));
        assertThat("There are four 'c's in the data", resultList, hasItem(new Document("_id", "c").append("value", 4.0)));
        assertThat("There is one 'd's in the data", resultList, hasItem(new Document("_id", "d").append("value", 1.0)));
    }

    @Test
    public void shouldMapIterableOfDocumentsIntoAnObjectOfYourChoice() {
        //given
        insertLabelData();

        //when
        //find all IDs greater than zero
        MongoIterable<Document> results = collection.find(new Document("_id", new Document("$gt", 0)))
                                              .sort(ascending("_id"))   // sort by ID
                                              .skip(1)                  // skip the first in the results
                                              .limit(6)                 // limit to 6 of the remaining results
                                              .mapReduce("  function(){ "
                                                         + "  for ( var i=0; i < this.labels.length; i++ ){ "
                                                         + "    emit( this.labels[i] , 1 ); "
                                                         + "  }"
                                                         + "}",

                                                         "  function(key,values){ "
                                                         + "  var sum=0; "
                                                         + "  for( var i=0; i < values.length; i++ ) "
                                                         + "    sum += values[i]; "
                                                         + "  return sum;"
                                                         + "}"
                                                        );
        // will perform Map Reduce on _ids 2-7

        //transforms Documents into LabelCount objects
        List<LabelCount> labelCounts = results.map(new Function<Document, LabelCount>() {
            @Override
            public LabelCount apply(final Document document) {
                return new LabelCount(document.getString("_id"), document.getDouble("value"));
            }
        })
                                              .into(new ArrayList<LabelCount>());

        //then
        assertThat("Transformed results should still be the same size as original results", labelCounts.size(), is(4));

        Collections.sort(labelCounts);
        assertThat("Should be LabelCount ordered by count ascending", labelCounts, contains(new LabelCount("d", 1),
                                                                                            new LabelCount("a", 2),
                                                                                            new LabelCount("c", 4),
                                                                                            new LabelCount("b", 5)));
    }

    private void insertCustomerData() {
        // see http://docs.mongodb.org/manual/core/map-reduce/
        collection.save(new Document("cust_id", "A123").append("amount", 500).append("status", "A"));
        collection.save(new Document("cust_id", "A123").append("amount", 250).append("status", "A"));
        collection.save(new Document("cust_id", "B212").append("amount", 200).append("status", "A"));
        collection.save(new Document("cust_id", "A123").append("amount", 300).append("status", "D"));
    }

    @Test
    public void shouldSumFilteredAmounts() {
        // Given
        insertCustomerData();

        // When
        //find all orders with status "A"
        MongoIterable<Document> results = collection.find(new Document("status", "A"))
                                                    .mapReduce("  function(){ "
                                                               + "  emit( this.cust_id, this.amount ); "
                                                               + "}",

                                                               "  function(key,values){ "
                                                               + "  return Array.sum(values);"
                                                               + "}"
                                                              );

        // Then
        List<Document> totalForOrdersWithStatusAPerCustomer = results.into(new ArrayList<Document>());
        assertThat(totalForOrdersWithStatusAPerCustomer.size(), is(2));

        assertThat(totalForOrdersWithStatusAPerCustomer, contains(new Document("_id", "A123").append("value", 750.0),
                                                                  new Document("_id", "B212").append("value", 200.0)));
    }

    @Test
    @Ignore("not implemented yet - waiting until fluent API is defined")
    public void shouldInsertMapReduceResultsIntoACollectionWhenOutputTypeIsNotInline() {
        fail("API not defined");
    }

    static class LabelCount implements Comparable<LabelCount> {
        private final String tag;
        private final double count;

        LabelCount(final String tag, final double count) {
            this.tag = tag;
            this.count = count;
        }

        @SuppressWarnings("RedundantIfStatement")
        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LabelCount labelCount = (LabelCount) o;

            if (Double.compare(labelCount.count, count) != 0) {
                return false;
            }
            if (!tag.equals(labelCount.tag)) {
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

        // So Collections.sort works.  Sorts by count.
        @Override
        public int compareTo(final LabelCount labelCount) {
            int cmp = Double.compare(count, labelCount.count);
            if (cmp != 0) {
                return cmp;
            }
            return tag.compareTo(labelCount.tag);
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
