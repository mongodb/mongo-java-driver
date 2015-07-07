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

package org.bson.codecs.configuration.mapper.conventions.entities;

import org.bson.codecs.configuration.mapper.ClassModel;
import org.bson.codecs.configuration.mapper.FieldModel;
import org.bson.codecs.configuration.mapper.conventions.Convention;
import org.bson.codecs.configuration.mapper.conventions.ConventionPack;

public class Rot13Convention implements Convention {
    @Override
    public void apply(final ClassModel classModel) {
        for (final FieldModel fieldModel : classModel.getFields()) {
            if (fieldModel.hasAnnotation(Secure.class)) {
                fieldModel.setConverter(new Rot13Converter());
            }
        }
    }

    @Override
    public String getPhase() {
        return ConventionPack.FIELD_MAPPING;
    }

}
