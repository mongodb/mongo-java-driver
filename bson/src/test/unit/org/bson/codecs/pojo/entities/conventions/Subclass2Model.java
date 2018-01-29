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

import org.bson.codecs.pojo.annotations.BsonDiscriminator;

@BsonDiscriminator
public class Subclass2Model extends SuperClassModel {
    private int integer;

    public int getInteger() {
        return integer;
    }

    public Subclass2Model setInteger(final int integer) {
        this.integer = integer;
        return this;
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

        Subclass2Model that = (Subclass2Model) o;

        return getInteger() == that.getInteger();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + getInteger();
        return result;
    }

    @Override
    public String toString() {
        return "Subclass2Model{"
                + "integer=" + integer
                + "} " + super.toString();
    }
}
