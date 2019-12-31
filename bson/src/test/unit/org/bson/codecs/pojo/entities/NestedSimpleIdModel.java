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

public class NestedSimpleIdModel {
    private String id;
    private SimpleIdModel nestedSimpleIdModel;

    public NestedSimpleIdModel(){
    }

    public NestedSimpleIdModel(final SimpleIdModel nestedSimpleIdModel) {
        this(null, nestedSimpleIdModel);
    }

    public NestedSimpleIdModel(final String id, final SimpleIdModel nestedSimpleIdModel) {
        this.id = id;
        this.nestedSimpleIdModel = nestedSimpleIdModel;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public SimpleIdModel getNestedSimpleIdModel() {
        return nestedSimpleIdModel;
    }

    public void setNestedSimpleIdModel(final SimpleIdModel nestedSimpleIdModel) {
        this.nestedSimpleIdModel = nestedSimpleIdModel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NestedSimpleIdModel that = (NestedSimpleIdModel) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return nestedSimpleIdModel != null ? nestedSimpleIdModel.equals(that.nestedSimpleIdModel) : that.nestedSimpleIdModel == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (nestedSimpleIdModel != null ? nestedSimpleIdModel.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NestedSimpleIdModel{"
                + "id='" + id + '\''
                + ", nestedSimpleIdModel=" + nestedSimpleIdModel
                + '}';
    }
}
