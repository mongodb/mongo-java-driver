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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MapGetterMutableModel {

    private final Map<String, Integer> mapField;

    public MapGetterMutableModel() {
        this.mapField = new HashMap<>();
    }

    public MapGetterMutableModel(final Map<String, Integer> mapField) {
        this.mapField = mapField;
    }

    public Map<String, Integer> getMapField() {
        return mapField;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        MapGetterMutableModel that = (MapGetterMutableModel) o;
        return Objects.equals(mapField, that.mapField);
    }

    @Override
    public int hashCode() {
        return mapField != null ? mapField.hashCode() : 0;
    }
}
