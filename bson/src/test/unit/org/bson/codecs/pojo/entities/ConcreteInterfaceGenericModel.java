/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo.entities;

public class ConcreteInterfaceGenericModel implements InterfaceGenericModel<String> {
    private String property;

    public ConcreteInterfaceGenericModel() {
    }

    public ConcreteInterfaceGenericModel(final String property) {
        this.property = property;
    }

    @Override
    public String getPropertyA() {
        return property;
    }

    @Override
    public void setPropertyA(final String property) {
        this.property = property;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConcreteInterfaceGenericModel that = (ConcreteInterfaceGenericModel) o;

        return property != null ? property.equals(that.property) : that.property == null;
    }

    @Override
    public int hashCode() {
        return property != null ? property.hashCode() : 0;
    }
}
