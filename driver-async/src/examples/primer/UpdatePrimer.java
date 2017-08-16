/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package primer;

import org.junit.Test;

// @import: start
import org.bson.Document;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.result.UpdateResult;
// @import: end

public class UpdatePrimer extends PrimerTestCase {

    @Test
    public void updateTopLevelFields() {
        // @begin: update-top-level-fields
        db.getCollection("restaurants").updateOne(new Document("name", "Juni"),
                new Document("$set", new Document("cuisine", "American (New)"))
                        .append("$currentDate", new Document("lastModified", true)),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        System.out.println("Operation Finished");
                        System.out.println(result);
                    }
                });

        /*
        // @post: start
            The updateOne operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified
        // @post: end
        */
        // @end: update-top-level-fields
    }

    @Test
    public void updateEmbeddedField() {
        // @begin: update-top-level-fields
        db.getCollection("restaurants").updateOne(new Document("restaurant_id", "41156888"),
                new Document("$set", new Document("address.street", "East 31st Street")),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        System.out.println("Operation Finished");
                        System.out.println(result);
                    }
                });

        /*
        // @post: start
            The updateOne operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified
        // @post: end
        */
        // @end: update-top-level-fields
    }


    @Test
    public void updateMultipleDocuments() {

        // @begin: update-multiple-documents
        db.getCollection("restaurants").updateMany(new Document("address.zipcode", "10016").append("cuisine", "Other"),
                new Document("$set", new Document("cuisine", "Category To Be Determined"))
                        .append("$currentDate", new Document("lastModified", true)),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        System.out.println("Operation Finished");
                        System.out.println(result);
                    }
                });

        /*
        // @post: start
            The updateMany operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified
        // @post: end
        */
        // @end: update-multiple-documents
    }
}
