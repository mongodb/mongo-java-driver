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

import java.util.List;
import java.util.Map;

public final class SimpleGenericsModel<T, V, Z> {
    private Integer myIntegerField;
    private T myGenericField;
    private List<V> myListField;
    private Map<String, Z> myMapField;

    public SimpleGenericsModel() {
    }

    public SimpleGenericsModel(final Integer myIntegerField, final T myGenericField, final List<V> myListField, final Map<String, Z>
            myMapField) {
        this.myIntegerField = myIntegerField;
        this.myGenericField = myGenericField;
        this.myListField = myListField;
        this.myMapField = myMapField;
    }

    public Integer getMyIntegerField() {
        return myIntegerField;
    }

    public void setMyIntegerField(final Integer myIntegerField) {
        this.myIntegerField = myIntegerField;
    }

    public T getMyGenericField() {
        return myGenericField;
    }

    public void setMyGenericField(final T myGenericField) {
        this.myGenericField = myGenericField;
    }

    public List<V> getMyListField() {
        return myListField;
    }

    public void setMyListField(final List<V> myListField) {
        this.myListField = myListField;
    }

    public Map<String, Z> getMyMapField() {
        return myMapField;
    }

    public void setMyMapField(final Map<String, Z> myMapField) {
        this.myMapField = myMapField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleGenericsModel<?, ?, ?> that = (SimpleGenericsModel<?, ?, ?>) o;

        if (getMyIntegerField() != null ? !getMyIntegerField().equals(that.getMyIntegerField()) : that.getMyIntegerField() != null) {
            return false;
        }
        if (getMyGenericField() != null ? !getMyGenericField().equals(that.getMyGenericField()) : that.getMyGenericField() != null) {
            return false;
        }
        if (getMyListField() != null ? !getMyListField().equals(that.getMyListField()) : that.getMyListField() != null) {
            return false;
        }
        if (getMyMapField() != null ? !getMyMapField().equals(that.getMyMapField()) : that.getMyMapField() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getMyIntegerField() != null ? getMyIntegerField().hashCode() : 0;
        result = 31 * result + (getMyGenericField() != null ? getMyGenericField().hashCode() : 0);
        result = 31 * result + (getMyListField() != null ? getMyListField().hashCode() : 0);
        result = 31 * result + (getMyMapField() != null ? getMyMapField().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SimpleGenericsModel{"
                + "myIntegerField=" + myIntegerField
                + ", myGenericField=" + myGenericField
                + ", myListField=" + myListField
                + ", myMapField=" + myMapField
                + "}";
    }
}
