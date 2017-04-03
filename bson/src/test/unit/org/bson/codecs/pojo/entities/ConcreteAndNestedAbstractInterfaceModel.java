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


import org.bson.codecs.pojo.annotations.Property;

public final class ConcreteAndNestedAbstractInterfaceModel extends AbstractInterfaceModel {
    private String name;

    @Property(useDiscriminator = true)
    private InterfaceBasedModel child;

    public ConcreteAndNestedAbstractInterfaceModel() {
    }

    public ConcreteAndNestedAbstractInterfaceModel(final String name, final InterfaceBasedModel child) {
        this.name = name;
        this.child = child;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConcreteAndNestedAbstractInterfaceModel that = (ConcreteAndNestedAbstractInterfaceModel) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (child != null ? !child.equals(that.child) : that.child != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (child != null ? child.hashCode() : 0);
        return result;
    }
}
