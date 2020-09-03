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

package org.bson.codecs.pojo.entities;

import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonRepresentation;

import java.util.Objects;

public class BsonRepresentationUnsupportedString {
    @BsonId
    private String id;

    @BsonRepresentation(BsonType.INT32)
    private String s;

    public BsonRepresentationUnsupportedString() {
    }

    public BsonRepresentationUnsupportedString(final String s) {
        this.id = "1";
        this.s = s;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public void setS(final String s) {
        this.s = s;
    }

    public String getId() {
        return id;
    }

    public String getS() {
        return s;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonRepresentationUnsupportedString that = (BsonRepresentationUnsupportedString) o;
        return Objects.equals(id, that.id) && Objects.equals(s, that.s);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, s);
    }
}
