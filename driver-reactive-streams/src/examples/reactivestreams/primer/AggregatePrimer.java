/*
 * Copyright 2008-present MongoDB, Inc.
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

package reactivestreams.primer;

// @import: start
import com.mongodb.reactivestreams.client.AggregatePublisher;
import org.bson.Document;
import org.junit.Test;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintDocumentSubscriber;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
// @import: end

public class AggregatePrimer extends PrimerTestCase {

    @Test
    public void  groupDocumentsByAFieldAndCalculateCount() {

        // @begin: group-documents-by-a-field-and-calculate-count
        // @code: start
        AggregatePublisher<Document> publisher = db.getCollection("restaurants").aggregate(singletonList(
                new Document("$group", new Document("_id", "$borough").append("count", new Document("$sum", 1)))));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document
        // @code: start
        ObservableSubscriber<Document> aggregateSubscriber = new PrintDocumentSubscriber();
        publisher.subscribe(aggregateSubscriber);
        aggregateSubscriber.await();
        // @code: end

        /*
        // @results: start
        { "_id" : "Missing", "count" : 51 }
        { "_id" : "Staten Island", "count" : 969 }
        { "_id" : "Manhattan", "count" : 10259 }
        { "_id" : "Brooklyn", "count" : 6086 }
        { "_id" : "Queens", "count" : 5656 }
        { "_id" : "Bronx", "count" : 2338 }
        // @results: end
       */

       // @end: group-documents-by-a-field-and-calculate-count
    }

    @Test
    public void filterAndGroupDocuments() {

        // @begin: filter-and-group-documents
        // @code: start
        AggregatePublisher<Document> publisher = db.getCollection("restaurants").aggregate(asList(
                new Document("$match", new Document("borough", "Queens").append("cuisine", "Brazilian")),
                new Document("$group", new Document("_id", "$address.zipcode").append("count", new Document("$sum", 1)))));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document
        // @code: start
        ObservableSubscriber<Document> aggregateSubscriber = new PrintDocumentSubscriber();
        publisher.subscribe(aggregateSubscriber);
        aggregateSubscriber.await();
        // @code: end

        /*
        // @results: start
        { "_id" : "11377", "count" : 1 }
        { "_id" : "11368", "count" : 1 }
        { "_id" : "11101", "count" : 2 }
        { "_id" : "11106", "count" : 3 }
        { "_id" : "11103", "count" : 1 }
        // @results: end
        */

        // @end: filter-and-group-documents
    }
}
