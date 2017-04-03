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

import org.bson.codecs.pojo.annotations.Discriminator;
import org.bson.codecs.pojo.annotations.Property;

@Discriminator("FieldWithMultipleTypeParamsModel")
public final class FieldWithMultipleTypeParamsModel<C, A, B> {

    @Property(useDiscriminator = true)
    private SimpleGenericsModel<A, B, C> simpleGenericsModel;

    public FieldWithMultipleTypeParamsModel() {
    }

    public FieldWithMultipleTypeParamsModel(final SimpleGenericsModel<A, B, C> simpleGenericsModel) {
        this.simpleGenericsModel = simpleGenericsModel;
    }

    /**
     * Returns the nested
     *
     * @return the nested
     */
    public SimpleGenericsModel<A, B, C> getSimpleGenericsModel() {
        return simpleGenericsModel;
    }

    /**
     * Sets the simpleGenericsModel
     *
     * @param simpleGenericsModel the simpleGenericsModel
     * @return this
     */
    public FieldWithMultipleTypeParamsModel<C, A, B> simpleGenericsModel(final SimpleGenericsModel<A, B, C> simpleGenericsModel) {
        this.simpleGenericsModel = simpleGenericsModel;
        return this;
    }

    @Override
    public String toString() {
        return "FieldWithMultipleTypeParamsModel{"
                + "nested=" + simpleGenericsModel
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldWithMultipleTypeParamsModel)) {
            return false;
        }

        FieldWithMultipleTypeParamsModel<?, ?, ?> that = (FieldWithMultipleTypeParamsModel<?, ?, ?>) o;

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
