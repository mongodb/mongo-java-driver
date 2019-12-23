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

public class InterfaceWithOverrideDefaultMethodModelImpl extends InterfaceModelAbstract implements InterfaceModelC {

    private String propertyC;

    public InterfaceWithOverrideDefaultMethodModelImpl() {
    }

    public InterfaceWithOverrideDefaultMethodModelImpl(final String propertyA, final String propertyC) {
        super(propertyA);
        this.propertyC = propertyC;
    }

    @Override
    public String getPropertyC() {
        return propertyC;
    }

    @Override
    public void setPropertyC(final String propertyC) {
        this.propertyC = propertyC;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InterfaceWithOverrideDefaultMethodModelImpl that = (InterfaceWithOverrideDefaultMethodModelImpl) o;

        if (getPropertyA() != null ? !getPropertyA().equals(that.getPropertyA()) : that.getPropertyA() != null) {
            return false;
        }

        if (getPropertyC() != null ? !getPropertyC().equals(that.getPropertyC()) : that.getPropertyC() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getPropertyA() != null ? getPropertyA().hashCode() : 0;
        result = 31 * result + getPropertyC() != null ? getPropertyC().hashCode() : 0;
        return result;
    }
}
