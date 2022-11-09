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

import java.util.Objects;

public class CreatorInSuperClassModelImpl extends CreatorInSuperClassModel {
    private final String propertyA;
    private final String propertyB;

    CreatorInSuperClassModelImpl(final String propertyA, final String propertyB) {
        this.propertyA = propertyA;
        this.propertyB = propertyB;
    }

    @Override
    public String getPropertyA() {
        return propertyA;
    }

    @Override
    public String getPropertyB() {
        return propertyB;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CreatorInSuperClassModelImpl that = (CreatorInSuperClassModelImpl) o;

        if (!Objects.equals(propertyA, that.propertyA)) {
            return false;
        }
        return Objects.equals(propertyB, that.propertyB);
    }

    @Override
    public int hashCode() {
        int result = propertyA != null ? propertyA.hashCode() : 0;
        result = 31 * result + (propertyB != null ? propertyB.hashCode() : 0);
        return result;
    }
}
