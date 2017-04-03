/*
 * Copyright 2017 MongoDB, Inc.
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

package org.bson.codecs.pojo.entities;

public final class GenericTreeModel<A, B> {

    private A field1;
    private B field2;
    private GenericTreeModel<A, B> left;
    private GenericTreeModel<A, B> right;

    public GenericTreeModel() {
    }

    public GenericTreeModel(final A field1, final B field2, final GenericTreeModel<A, B> left, final GenericTreeModel<A, B> right) {
        this.field1 = field1;
        this.field2 = field2;
        this.left = left;
        this.right = right;
    }

    public A getField1() {
        return field1;
    }

    public void setField1(final A field1) {
        this.field1 = field1;
    }

    public B getField2() {
        return field2;
    }

    public void setField2(final B field2) {
        this.field2 = field2;
    }

    public GenericTreeModel<A, B> getLeft() {
        return left;
    }

    public void setLeft(final GenericTreeModel<A, B> left) {
        this.left = left;
    }

    public GenericTreeModel<A, B> getRight() {
        return right;
    }

    public void setRight(final GenericTreeModel<A, B> right) {
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

        GenericTreeModel<?, ?> that = (GenericTreeModel<?, ?>) o;

        if (getField1() != null ? !getField1().equals(that.getField1()) : that.getField1() != null) {
            return false;
        }
        if (getField2() != null ? !getField2().equals(that.getField2()) : that.getField2() != null) {
            return false;
        }
        if (getLeft() != null ? !getLeft().equals(that.getLeft()) : that.getLeft() != null) {
            return false;
        }
        if (getRight() != null ? !getRight().equals(that.getRight()) : that.getRight() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getField1() != null ? getField1().hashCode() : 0;
        result = 31 * result + (getField2() != null ? getField2().hashCode() : 0);
        result = 31 * result + (getLeft() != null ? getLeft().hashCode() : 0);
        result = 31 * result + (getRight() != null ? getRight().hashCode() : 0);
        return result;
    }
}
