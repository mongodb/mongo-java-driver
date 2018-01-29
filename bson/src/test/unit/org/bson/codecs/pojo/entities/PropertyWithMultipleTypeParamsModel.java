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

import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonProperty;

@BsonDiscriminator("PropertyWithMultipleTypeParamsModel")
public final class PropertyWithMultipleTypeParamsModel<C, A, B> {

    @BsonProperty(useDiscriminator = true)
    private SimpleGenericsModel<A, B, C> simpleGenericsModel;

    public PropertyWithMultipleTypeParamsModel() {
    }

    public PropertyWithMultipleTypeParamsModel(final SimpleGenericsModel<A, B, C> simpleGenericsModel) {
        this.simpleGenericsModel = simpleGenericsModel;
    }

    public SimpleGenericsModel<A, B, C> getSimpleGenericsModel() {
        return simpleGenericsModel;
    }

    public void setSimpleGenericsModel(final SimpleGenericsModel<A, B, C> simpleGenericsModel) {
        this.simpleGenericsModel = simpleGenericsModel;
    }

    @Override
    public String toString() {
        return "PropertyWithMultipleTypeParamsModel{"
                + "nested=" + simpleGenericsModel
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PropertyWithMultipleTypeParamsModel)) {
            return false;
        }

        PropertyWithMultipleTypeParamsModel<?, ?, ?> that = (PropertyWithMultipleTypeParamsModel<?, ?, ?>) o;

        if (getSimpleGenericsModel() != null ? !getSimpleGenericsModel().equals(that.getSimpleGenericsModel())
                : that.getSimpleGenericsModel() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getSimpleGenericsModel() != null ? getSimpleGenericsModel().hashCode() : 0;
    }
}
