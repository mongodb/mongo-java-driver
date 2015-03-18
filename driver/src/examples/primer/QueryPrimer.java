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

// @imports: start
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.client.FindIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.ascending;
import static java.util.Arrays.asList;
// @imports: end


public class QueryPrimer extends PrimerTestCase {

    @Test
    public void queryAll() {
        // @begin: query-all
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find();
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end
        // @end: query-all
    }


    @Test
    public void logicalAnd() {

        // @begin: logical-and
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("cuisine", "Italian").append("address.zipcode", "10075"));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(and(eq("cuisine", "Italian"), eq("address.zipcode", "10075")));
        // @code: end

        // @end: logical-and
    }

    @Test
    public void logicalOr() {

        // @begin: logical-or
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("$or", asList(new Document("cuisine", "Italian"),
                        new Document("address.zipcode", "10075"))));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(or(eq("cuisine", "Italian"), eq("address.zipcode", "10075")));
        // @code: end

        // @end: logical-or
    }

    @Test
    public void queryTopLevelField() {
        // @begin: query-top-level-field
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("borough", "Manhattan"));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(eq("borough", "Manhattan"));
        // @code: end
        // @end: query-top-level-field
    }

    @Test
    public void queryEmbeddedDocument() {
        // @begin: query-embedded-document
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("address.zipcode", "10075"));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(eq("address.zipcode", "10075"));
        // @code: end
        // @end: query-embedded-document
    }

    @Test
    public void queryFieldInArray() {
        // @begin: query-field-in-array
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("grades.grade", "B"));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(eq("grades.grade", "B"));
        // @code: end
        // @end: query-field-in-array
    }

    @Test
    public void greaterThan() {
        // @begin: greater-than
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("grades.score", new Document("$gt", 30)));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(gt("grades.score", 30));
        // @code: end
        // @end: greater-than
    }

    @Test
    public void lessThan() {
        // @begin: less-than
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find(
                new Document("grades.score", new Document("$lt", 10)));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document.
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify building queries the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find(lt("grades.score", 10));
        // @code: end
        // @end: less-than
    }


    @Test
    public void sort() {
        // @begin: sort
        // @code: start
        FindIterable<Document> iterable = db.getCollection("restaurants").find()
                .sort(new Document("borough", 1).append("address.zipcode", 1));
        // @code: end

        // @pre: Iterate the results and apply a block to each resulting document
        // @code: start
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        // @code: end

        // @pre: To simplify sorting fields the Java driver provides static helpers
        // @code: start
        db.getCollection("restaurants").find().sort(ascending("borough", "address.zipcode"));
        // @code: end
        // @end: sort
    }
}
