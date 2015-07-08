/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package org.bson.codecs.configuration.mapper.conventions;

import org.bson.codecs.configuration.mapper.ClassModel;
import org.bson.codecs.configuration.mapper.FieldModel;
import org.bson.codecs.configuration.mapper.Weights;

class CustomConventionPack extends DefaultConventionPack {
    public CustomConventionPack() {
        addConvention(new SnakeCaseConvention());
    }

    static class SnakeCaseConvention implements Convention {
        @Override
        public void apply(final ClassModel model) {
            for (final FieldModel fieldModel : model.getFields()) {
                fieldModel.setName(Weights.USER_CONVENTION, snake(fieldModel.getFieldName()));
            }
        }

        private String snake(final String name) {
            return name.replaceAll("([A-Z])", "_$1").toLowerCase();
        }

        @Override
        public String getPhase() {
            return FIELD_MAPPING;
        }
    }
}
