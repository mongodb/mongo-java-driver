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

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

final class InstanceCreatorImpl<T> implements InstanceCreator<T> {
    private final CreatorExecutable<T> creatorExecutable;
    private final Map<PropertyModel<?>, Object> cachedValues;
    private final Map<String, Integer> properties;
    private final Object[] params;

    private T newInstance;

    InstanceCreatorImpl(final CreatorExecutable<T> creatorExecutable) {
        this.creatorExecutable = creatorExecutable;
        if (creatorExecutable.getProperties().isEmpty()) {
            this.cachedValues = null;
            this.properties = null;
            this.params = null;
            this.newInstance = creatorExecutable.getInstance();
        } else {
            this.cachedValues = new HashMap<>();
            this.properties = new HashMap<>();

            for (int i = 0; i < creatorExecutable.getProperties().size(); i++) {
                if (creatorExecutable.getIdPropertyIndex() != null && creatorExecutable.getIdPropertyIndex() == i) {
                    this.properties.put(ClassModelBuilder.ID_PROPERTY_NAME, creatorExecutable.getIdPropertyIndex());
                } else {
                    this.properties.put(creatorExecutable.getProperties().get(i).value(), i);
                }
            }

            this.params = new Object[properties.size()];
        }
    }

    @Override
    public <S> void set(final S value, final PropertyModel<S> propertyModel) {
        if (newInstance != null) {
            propertyModel.getPropertyAccessor().set(newInstance, value);
        } else {
            if (!properties.isEmpty()) {
                String propertyName = propertyModel.getWriteName();

                if (!properties.containsKey(propertyName)) {
                    // Support legacy BsonProperty settings where the property name was used instead of the write name.
                    propertyName = propertyModel.getName();
                }

                Integer index = properties.get(propertyName);
                if (index != null) {
                    params[index] = value;
                }
                properties.remove(propertyName);
            }

            if (properties.isEmpty()) {
                constructInstanceAndProcessCachedValues();
            } else {
                cachedValues.put(propertyModel, value);
            }
        }
    }

    @Override
    public T getInstance() {
        if (newInstance == null) {
            try {
                for (Map.Entry<String, Integer> entry : properties.entrySet()) {
                    params[entry.getValue()] = null;
                }
                constructInstanceAndProcessCachedValues();
            } catch (CodecConfigurationException e) {
                throw new CodecConfigurationException(format("Could not construct new instance of: %s. "
                                + "Missing the following properties: %s",
                        creatorExecutable.getType().getSimpleName(), properties.keySet()), e);
            }
        }
        return newInstance;
    }

    private void constructInstanceAndProcessCachedValues() {
        try {
            newInstance = creatorExecutable.getInstance(params);
        } catch (Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }

        for (Map.Entry<PropertyModel<?>, Object> entry : cachedValues.entrySet()) {
            setPropertyValue(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private <S> void setPropertyValue(final PropertyModel<S> propertyModel, final Object value) {
        set((S) value, propertyModel);
    }
}
