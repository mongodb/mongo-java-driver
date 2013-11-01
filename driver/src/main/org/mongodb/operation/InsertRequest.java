/*
 * Copyright (c) 2008 - 2013 MongoDB Inc. <http://mongodb.com>
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

package org.mongodb.operation;

public class InsertRequest<T> extends WriteRequest {
    private final T document;

    public InsertRequest(final T document) {
        this.document = document;
    }

    public InsertRequest(final InsertRequest<T> insertRequest, final int startPos) {
        throw new UnsupportedOperationException();

//        documents = insert.getDocuments().subList(startPos, insert.getDocuments().size());
    }

    public T getDocument() {
        return document;
    }

    @Override
    public Type getType() {
        return Type.INSERT;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final InsertRequest insertRequest = (InsertRequest) o;

        if (document != null ? !document.equals(insertRequest.document) : insertRequest.document != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return document != null ? document.hashCode() : 0;
    }
}

