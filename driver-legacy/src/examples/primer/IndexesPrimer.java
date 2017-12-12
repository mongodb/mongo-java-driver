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
// @imports: end

public class IndexesPrimer extends PrimerTestCase {

    @Test
    public void  singleFieldIndex() {

        // @begin: single-field-index
        // @code: start
        db.getCollection("restaurants").createIndex(new Document("cuisine", 1));
        // @code: end

        // @post: The method does not return a result.
        // @end: single-field-index
    }

    @Test
    public void  createCompoundIndex() {
        // @begin: create-compound-index
        // @code: start
        db.getCollection("restaurants").createIndex(new Document("cuisine", 1).append("address.zipcode", -1));
        // @code: end

        // @post: The method does not return a result.
        // @end: create-compound-index
    }
}
