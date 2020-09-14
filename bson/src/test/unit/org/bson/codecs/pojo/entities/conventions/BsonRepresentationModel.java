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

package org.bson.codecs.pojo.entities.conventions;

import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.bson.types.ObjectId;

import java.util.Objects;

public class BsonRepresentationModel {
    @BsonRepresentation(BsonType.OBJECT_ID)
    private String id;

    private int age;

    public BsonRepresentationModel() {}

    public BsonRepresentationModel(final int age) {
        id = new ObjectId("111111111111111111111111").toHexString();
        this.age = age;
    }

    public BsonRepresentationModel(final String id, final int age) {
        this.id = id;
        this.age = age;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(final int age) {
        this.age = age;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BsonRepresentationModel that = (BsonRepresentationModel) o;
        return age == that.age && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, age);
    }

}
