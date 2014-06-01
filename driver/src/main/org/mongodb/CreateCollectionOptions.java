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

package org.mongodb;

public class CreateCollectionOptions {
    private final String collectionName;
    private final boolean autoIndex;
    private final long maxDocuments;
    private final boolean capped;
    private final long sizeInBytes;

    public CreateCollectionOptions(final String collectionName) {
        this(collectionName, false, 0, true);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes) {
        this(collectionName, capped, sizeInBytes, true);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes,
                                   final boolean autoIndex) {
        this(collectionName, capped, sizeInBytes, autoIndex, 0);
    }

    public CreateCollectionOptions(final String collectionName, final boolean capped, final long sizeInBytes,
                                   final boolean autoIndex, final long maxDocuments) {
        this.collectionName = collectionName;
        this.capped = capped;
        this.sizeInBytes = sizeInBytes;
        this.autoIndex = autoIndex;
        this.maxDocuments = maxDocuments;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public boolean isAutoIndex() {
        return autoIndex;
    }

    public long getMaxDocuments() {
        return maxDocuments;
    }

    public boolean isCapped() {
        return capped;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }
}
