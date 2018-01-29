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

public final class InvalidMapModel {
    private Map<Integer, Integer> invalidMap;

    public InvalidMapModel() {
    }

    public InvalidMapModel(final Map<Integer, Integer> invalidMap) {
        this.invalidMap = invalidMap;
    }

    public Map<Integer, Integer> getInvalidMap() {
        return invalidMap;
    }

    public void setInvalidMap(final Map<Integer, Integer> invalidMap) {
        this.invalidMap = invalidMap;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InvalidMapModel that = (InvalidMapModel) o;

        return invalidMap != null ? invalidMap.equals(that.invalidMap) : that.invalidMap == null;
    }

    @Override
    public int hashCode() {
        return invalidMap != null ? invalidMap.hashCode() : 0;
    }
}
