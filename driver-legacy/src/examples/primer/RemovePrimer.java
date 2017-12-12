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
// @import: end

public class RemovePrimer extends PrimerTestCase {

    @Test
    public void removeMatchingDocuments() {
        // @begin: remove-matching-documents
        // @code: start
        db.getCollection("restaurants").deleteMany(new Document("borough", "Manhattan"));
        // @code: end

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
        // @code: start
        db.getCollection("restaurants").deleteMany(new Document());
        // @code: end

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
        // @code: start
        db.getCollection("restaurants").drop();
        // @code: end
        // @end: drop-collection
    }
}
