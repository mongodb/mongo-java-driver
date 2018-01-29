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

package org.bson;

/**
 * Represents the value associated with the BSON Undefined type.  All values of this type are identical.  Note that this type has been
 * deprecated in the BSON specification.
 *
 * @see <a href="http://bsonspec.org/spec.html">BSON Spec</a>
 * @see org.bson.BsonType#UNDEFINED
 * @since 3.0
 */
public final class BsonUndefined extends BsonValue {

    @Override
    public BsonType getBsonType() {
        return BsonType.UNDEFINED;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
