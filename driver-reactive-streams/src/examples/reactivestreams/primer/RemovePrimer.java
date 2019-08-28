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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.reactivestreams.client.Success;
import org.bson.Document;
import org.junit.Test;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintSubscriber;
// @import: end

public class RemovePrimer extends PrimerTestCase {

    @Test
    public void removeMatchingDocuments() {
        // @begin: remove-matching-documents
        ObservableSubscriber<DeleteResult> deleteSubscriber = new PrintSubscriber<>("Update complete: %s");
        db.getCollection("restaurants").deleteMany(new Document("borough", "Manhattan"))
                .subscribe(deleteSubscriber);
        deleteSubscriber.await();

        /*
        // @post: start
            The deleteMany operation returns a ``DeleteResult`` which contains information about the operation.
            The ``getDeletedCount`` method returns number of documents deleted.
        // @post: end
        */
        // @end: remove-matching-documents
    }

    @Test
    public void removeAllDocuments() {
        // @begin: remove-all-documents
        ObservableSubscriber<DeleteResult> deleteSubscriber = new PrintSubscriber<>("Update complete: %s");
        db.getCollection("restaurants").deleteMany(new Document())
                .subscribe(deleteSubscriber);
        deleteSubscriber.await();

        /*
        // @post: start
            The deleteMany operation returns a ``DeleteResult`` which contains information about the operation.
            The ``getDeletedCount`` method returns number of documents deleted.
        // @post: end
        */
        // @end: remove-all-documents
    }

    @Test
    public void dropCollection() {
        // @begin: drop-collection
        ObservableSubscriber<Success> successSubscriber = new OperationSubscriber<>();
        db.getCollection("restaurants").drop()
                .subscribe(successSubscriber);
        successSubscriber.await();
        // @end: drop-collection
    }
}
