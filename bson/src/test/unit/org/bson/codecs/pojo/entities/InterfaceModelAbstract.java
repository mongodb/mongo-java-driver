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

public abstract class InterfaceModelAbstract implements InterfaceModelA {

    private String propertyA;

    public InterfaceModelAbstract() {
    }

    public InterfaceModelAbstract(final String propertyA) {
        this.propertyA = propertyA;
    }

    @Override
    public String getPropertyA() {
        return propertyA;
    }

    @Override
    public void setPropertyA(final String property) {
        this.propertyA = property;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InterfaceModelAbstract that = (InterfaceModelAbstract) o;

        if (getPropertyA() != null ? !getPropertyA().equals(that.getPropertyA()) : that.getPropertyA() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getPropertyA() != null ? getPropertyA().hashCode() : 0;
    }
}
