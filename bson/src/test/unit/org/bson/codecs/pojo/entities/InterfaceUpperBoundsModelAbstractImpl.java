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

public class InterfaceUpperBoundsModelAbstractImpl extends InterfaceUpperBoundsModelAbstract {
    private String name;
    private InterfaceModelImpl nestedModel;

    public InterfaceUpperBoundsModelAbstractImpl() {
    }

    public InterfaceUpperBoundsModelAbstractImpl(final String name, final InterfaceModelImpl nestedModel) {
        this.name = name;
        this.nestedModel = nestedModel;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public InterfaceModelImpl getNestedModel() {
        return nestedModel;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setNestedModel(final InterfaceModelImpl nestedModel) {
        this.nestedModel = nestedModel;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InterfaceUpperBoundsModelAbstractImpl that = (InterfaceUpperBoundsModelAbstractImpl) o;

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        return nestedModel != null ? nestedModel.equals(that.nestedModel) : that.nestedModel == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (nestedModel != null ? nestedModel.hashCode() : 0);
        return result;
    }
}
