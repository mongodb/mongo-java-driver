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

public class AnnotationBsonRepresentation {
    private String id;
    private String friendId;

    @BsonRepresentation(BsonType.OBJECT_ID)
    private String parentId;

    private int age;

    public AnnotationBsonRepresentation() {}

    public AnnotationBsonRepresentation(final int age) {
        id = new ObjectId("111111111111111111111111").toHexString();
        friendId = "";
        parentId = "";
        this.age = age;
    }

    public AnnotationBsonRepresentation(final String id, final String friendId, final String parentId, final int age) {
        this.id = id;
        this.friendId = friendId;
        this.parentId = parentId;
        this.age = age;
    }

    @BsonRepresentation(BsonType.OBJECT_ID)
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getFriendId() {
        return friendId;
    }

    @BsonRepresentation(BsonType.OBJECT_ID)
    public void setFriendId(final String friendId) {
        this.friendId = friendId;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(final String parentId) {
        this.parentId = parentId;
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
        AnnotationBsonRepresentation that = (AnnotationBsonRepresentation) o;
        return age == that.age && Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, age);
    }

}
