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

import java.util.Map;

public class MapStringObjectModel {
    private Map<String, Object> map;

    public MapStringObjectModel() {
    }

    public MapStringObjectModel(final Map<String, Object> map) {
        this.map = map;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(final Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MapStringObjectModel that = (MapStringObjectModel) o;

        return getMap() != null ? getMap().equals(that.getMap()) : that.getMap() == null;
    }

    @Override
    public int hashCode() {
        return getMap() != null ? getMap().hashCode() : 0;
    }
}
