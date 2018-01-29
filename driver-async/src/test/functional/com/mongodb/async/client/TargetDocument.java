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
 */

package com.mongodb.async.client;

import org.bson.Document;
import org.bson.types.ObjectId;

public class TargetDocument {
    private ObjectId id;
    private String x;

    public TargetDocument(final Document document) {
        this((ObjectId) document.get("_id"), document.get("x").toString());
    }

    public TargetDocument(final ObjectId id, final String x) {
        this.id = id;
        this.x = x;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public String getX() {
        return x;
    }

    public void setX(final String x) {
        this.x = x;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TargetDocument that = (TargetDocument) o;

        if (!id.equals(that.id)) {
            return false;
        }
        if (!x.equals(that.x)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + x.hashCode();
        return result;
    }
}
