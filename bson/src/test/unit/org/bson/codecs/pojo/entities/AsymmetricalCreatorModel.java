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

import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonProperty;

public final class AsymmetricalCreatorModel {
    private final String baz;

    @BsonCreator
    public AsymmetricalCreatorModel(@BsonProperty("a") final String a, @BsonProperty("b") final String b) {
        this.baz = a + b;
    }

    public String getBaz() {
        return baz;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AsymmetricalCreatorModel that = (AsymmetricalCreatorModel) o;

        if (getBaz() != null ? !getBaz().equals(that.getBaz()) : that.getBaz() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getBaz() != null ? getBaz().hashCode() : 0;
    }
}
