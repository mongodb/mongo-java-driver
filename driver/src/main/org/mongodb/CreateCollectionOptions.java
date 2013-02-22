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

public class CreateCollectionOptions {
    private final Document createDocument;
    private final String collectionName;

    public CreateCollectionOptions(final String collectionName) {
        this(collectionName, false, 0, true);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final int sizeInBytes) {
        this(collectionName, capped, sizeInBytes, true);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final int sizeInBytes,
                                   final boolean autoIndex) {
        this(collectionName, capped, sizeInBytes, autoIndex, 0);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final int sizeInBytes,
                                   final boolean autoIndex, final int maxDocuments) {
        this.collectionName = collectionName;
        createDocument = new Document("create", collectionName);
        createDocument.put("capped", capped);
        //I want this to be >0 (seems correct) but for backwards compatibility with some of the tests had to change this
        if (sizeInBytes != 0) {
            createDocument.put("size", sizeInBytes);
        }
        if (capped) {
            createDocument.put("autoIndexId", autoIndex);
            if (maxDocuments > 0) {
                createDocument.put("max", maxDocuments);
            }
        }
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Document asDocument() {
        return createDocument;
    }
}
