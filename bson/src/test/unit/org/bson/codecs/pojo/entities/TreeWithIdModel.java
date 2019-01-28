/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo.entities;

import org.bson.types.ObjectId;

public class TreeWithIdModel {
    private ObjectId id;
    private String level;
    private TreeWithIdModel left;
    private TreeWithIdModel right;

    public TreeWithIdModel() {
    }

    public TreeWithIdModel(final String level) {
        this(null, level, null, null);
    }

    public TreeWithIdModel(final String level, final TreeWithIdModel left, final TreeWithIdModel right) {
        this(null, level, left, right);
    }

    public TreeWithIdModel(final ObjectId id, final String level, final TreeWithIdModel left, final TreeWithIdModel right) {
        this.id = id;
        this.level = level;
        this.left = left;
        this.right = right;
    }

    public ObjectId getId() {
        return id;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(final String level) {
        this.level = level;
    }

    public TreeWithIdModel getLeft() {
        return left;
    }

    public void setLeft(final TreeWithIdModel left) {
        this.left = left;
    }

    public TreeWithIdModel getRight() {
        return right;
    }

    public void setRight(final TreeWithIdModel right) {
        this.right = right;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TreeWithIdModel that = (TreeWithIdModel) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (level != null ? !level.equals(that.level) : that.level != null) {
            return false;
        }
        if (left != null ? !left.equals(that.left) : that.left != null) {
            return false;
        }
        return right != null ? right.equals(that.right) : that.right == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (level != null ? level.hashCode() : 0);
        result = 31 * result + (left != null ? left.hashCode() : 0);
        result = 31 * result + (right != null ? right.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TreeWithIdModel{"
                + "id=" + id
                + ", level=" + level
                + ", left=" + left
                + ", right=" + right
                + '}';
    }
}
