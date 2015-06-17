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
import org.bson.codecs.configuration.mapper.MethodModel;

import java.util.List;

/**
 * This convention ensures only fields that are private, nonstatic, nontransient, and nonfinal that have a proper getter and setter as per
 * the Beans Specification are included in BSON processing.
 *
 * @see <a target="_blank" href="http://www.oracle.com/technetwork/articles/javaee/spec-136004.html">JavaBeans Spec</a>
 */
public class BeanPropertiesConvention implements Convention {
    @Override
    public Integer getWeight() {
        return 100;
    }

    @Override
    public String getPhase() {
        return ConventionPack.FIELD_MAPPING;
    }

    @Override
    public void apply(final ClassModel model) {
        final List<FieldModel> resolvedFields = model.getFields();
        for (final FieldModel field : resolvedFields) {
            if (field.isPrivate() && !field.isTransient() && !field.isFinal()) {
                final String name = field.getName();
                final String propertyName = name.substring(0, 1).toUpperCase() + name.substring(1);

                if (!validGetter(model, field, propertyName) || !validSetter(model, field, propertyName)) {
                    field.setIncluded(getWeight(), false);
                }
            } else {
                field.setIncluded(getWeight(), false);
            }
        }
    }

    private boolean validGetter(final ClassModel model, final FieldModel field, final String propertyName) {
        final List<MethodModel> getters = model.getMethods("get" + propertyName);
        return getters != null
               && getters.size() == 1
               && getters.get(0).getReturnType().equals(field.getType());
    }

    private boolean validSetter(final ClassModel model, final FieldModel field, final String propertyName) {
        final List<MethodModel> setters = model.getMethods("set" + propertyName);
        return setters != null
               && setters.size() == 1
               && setters.get(0).getArgumentCount() == 1
               && setters.get(0).getArgumentType(0).equals(field.getType());
    }
}
