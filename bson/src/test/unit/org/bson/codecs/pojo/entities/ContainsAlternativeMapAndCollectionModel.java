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

import org.bson.BsonArray;
import org.bson.BsonDocument;

public final class ContainsAlternativeMapAndCollectionModel {
    private BsonArray customList;
    private BsonDocument customMap;

    public ContainsAlternativeMapAndCollectionModel() {
    }

    public ContainsAlternativeMapAndCollectionModel(final BsonDocument source) {
        this.customList = source.getArray("customList");
        this.customMap = source.getDocument("customMap");
    }

    public void setCustomList(final BsonArray customList) {
        this.customList = customList;
    }

    public void setCustomMap(final BsonDocument customMap) {
        this.customMap = customMap;
    }

    public BsonArray getCustomList() {
        return customList;
    }

    public BsonDocument getCustomMap() {
        return customMap;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ContainsAlternativeMapAndCollectionModel that = (ContainsAlternativeMapAndCollectionModel) o;

        if (getCustomList() != null ? !getCustomList().equals(that.getCustomList()) : that.getCustomList() != null) {
            return false;
        }
        if (getCustomMap() != null ? !getCustomMap().equals(that.getCustomMap()) : that.getCustomMap() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getCustomList() != null ? getCustomList().hashCode() : 0;
        result = 31 * result + (getCustomMap() != null ? getCustomMap().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ContainsAlternativeMapAndCollectionModel{"
                + "customList=" + customList
                + ", customMap=" + customMap
                + "}";
    }
}
