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

import static java.util.Arrays.asList;
// @import: end

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assume.assumeTrue;


public class UpdatePrimer extends PrimerTestCase {

    @Test
    public void updateTopLevelFields() {
        assumeTrue(serverVersionAtLeast(2, 6));

        // @begin: update-top-level-fields
        // @code: start
        db.getCollection("restaurants").updateOne(new Document("name", "Juni"),
                new Document("$set", new Document("cuisine", "American (New)"))
                    .append("$currentDate", new Document("lastModified", true)));
        // @code: end

        /*
        // @post: start
            The updateOne operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified.
        // @post: end
        */
        // @end: update-top-level-fields
    }

    @Test
    public void updateEmbeddedField() {
        // @begin: update-embedded-field
        // @code: start
        db.getCollection("restaurants").updateOne(new Document("restaurant_id", "41156888"),
                new Document("$set", new Document("address.street", "East 31st Street")));

        // @code: end
        /*
        // @post: start
            The updateOne operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified.
        // @post: end
        */
        // @end: update-embedded-field
    }


    @Test
    public void updateMultipleDocuments() {
        assumeTrue(serverVersionAtLeast(2, 6));

        // @begin: update-multiple-documents
        // @code: start
        db.getCollection("restaurants").updateMany(new Document("address.zipcode", "10016").append("cuisine", "Other"),
                new Document("$set", new Document("cuisine", "Category To Be Determined"))
                        .append("$currentDate", new Document("lastModified", true)));
        // @code: end

        /*
        // @post: start
            The updateMany operation returns a ``UpdateResult`` which contains information about the operation.
            The ``getModifiedCount`` method returns the number of documents modified.
        // @post: end
        */
        // @end: update-multiple-documents
    }

    @Test
    public void replaceDocument() {
        assumeTrue(serverVersionAtLeast(2, 6));

        // @begin: replace-document
        // @code: start
        db.getCollection("restaurants").replaceOne(new Document("restaurant_id", "41704620"),
                new Document("address",
                        new Document()
                                .append("street", "2 Avenue")
                                .append("zipcode", "10075")
                                .append("building", "1480")
                                .append("coord", asList(-73.9557413, 40.7720266)))
                        .append("name", "Vella 2"));
       // @code: end
       /*
       // @post: start
           The replaceOne operation returns a ``UpdateResult`` which contains information about the operation.
           The ``getModifiedCount`` method returns the number of documents modified.
       // @post: end
       */

       // @end: replace-document
    }
}
