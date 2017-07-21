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


import org.bson.codecs.pojo.annotations.BsonProperty;

public final class ConcreteAndNestedAbstractInterfaceModel extends AbstractInterfaceModel {
    @BsonProperty(useDiscriminator = true)
    private InterfaceBasedModel child;

    public ConcreteAndNestedAbstractInterfaceModel() {
        super();
    }

    public ConcreteAndNestedAbstractInterfaceModel(final String name, final InterfaceBasedModel child) {
        super(name);
        this.child = child;
    }

    public InterfaceBasedModel getChild() {
        return child;
    }

    public void setChild(final InterfaceBasedModel child) {
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
        if (!super.equals(o)) {
            return false;
        }

        ConcreteAndNestedAbstractInterfaceModel that = (ConcreteAndNestedAbstractInterfaceModel) o;

        if (getChild() != null ? !getChild().equals(that.getChild()) : that.getChild() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (getChild() != null ? getChild().hashCode() : 0);
        return result;
    }
}
