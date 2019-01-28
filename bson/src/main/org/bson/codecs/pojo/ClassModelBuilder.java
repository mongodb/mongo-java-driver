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

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.pojo.Conventions.DEFAULT_CONVENTIONS;
import static org.bson.codecs.pojo.PojoBuilderHelper.configureClassModelBuilder;
import static org.bson.codecs.pojo.PojoBuilderHelper.stateNotNull;

/**
 * A builder for programmatically creating {@code ClassModels}.
 *
 * @param <T> The type of the class the ClassModel represents
 * @since 3.5
 * @see ClassModel
 */
public class ClassModelBuilder<T> {
    static final String ID_PROPERTY_NAME = "_id";
    private final List<PropertyModelBuilder<?>> propertyModelBuilders = new ArrayList<PropertyModelBuilder<?>>();
    private IdGenerator<?> idGenerator;
    private InstanceCreatorFactory<T> instanceCreatorFactory;
    private Class<T> type;
    private Map<String, TypeParameterMap> propertyNameToTypeParameterMap = emptyMap();
    private List<Convention> conventions = DEFAULT_CONVENTIONS;
    private List<Annotation> annotations = emptyList();
    private boolean discriminatorEnabled;
    private String discriminator;
    private String discriminatorKey;
    private String idPropertyName;

    ClassModelBuilder(final Class<T> type) {
        configureClassModelBuilder(this, notNull("type", type));
    }

    /**
     * Sets the IdGenerator for the ClassModel
     *
     * @param idGenerator the IdGenerator
     * @return this
     * @since 3.10
     */
    public ClassModelBuilder<T> idGenerator(final IdGenerator<?> idGenerator) {
        this.idGenerator = idGenerator;
        return this;
    }

    /**
     * @return the IdGenerator for the ClassModel, or null if not set
     * @since 3.10
     */
    public IdGenerator<?> getIdGenerator() {
        return idGenerator;
    }

    /**
     * Sets the InstanceCreatorFactory for the ClassModel
     *
     * @param instanceCreatorFactory the InstanceCreatorFactory
     * @return this
     */
    public ClassModelBuilder<T> instanceCreatorFactory(final InstanceCreatorFactory<T> instanceCreatorFactory) {
        this.instanceCreatorFactory = notNull("instanceCreatorFactory", instanceCreatorFactory);
        return this;
    }

    /**
     * @return the InstanceCreatorFactory for the ClassModel
     */
    public InstanceCreatorFactory<T> getInstanceCreatorFactory() {
        return instanceCreatorFactory;
    }

    /**
     * Sets the type of the model
     *
     * @param type the type of the class
     * @return the builder to configure the class being modeled
     */
    public ClassModelBuilder<T> type(final Class<T> type) {
        this.type = notNull("type", type);
        return this;
    }

    /**
     * @return the type if set or null
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Sets the conventions to apply to the model
     *
     * @param conventions a list of conventions
     * @return this
     */
    public ClassModelBuilder<T> conventions(final List<Convention> conventions) {
        this.conventions = notNull("conventions", conventions);
        return this;
    }

    /**
     * @return the conventions o apply to the model
     */
    public List<Convention> getConventions() {
        return conventions;
    }

    /**
     * Sets the annotations for the model
     *
     * @param annotations a list of annotations
     * @return this
     */
    public ClassModelBuilder<T> annotations(final List<Annotation> annotations) {
        this.annotations = notNull("annotations", annotations);
        return this;
    }

    /**
     * @return the annotations on the modeled type if set or null
     */
    public List<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * Sets the discriminator to be used when storing instances of the modeled type
     *
     * @param discriminator the discriminator value
     * @return this
     */
    public ClassModelBuilder<T> discriminator(final String discriminator) {
        this.discriminator = discriminator;
        return this;
    }

    /**
     * @return the discriminator to be used when storing instances of the modeled type or null if not set
     */
    public String getDiscriminator() {
        return discriminator;
    }

    /**
     * Sets the discriminator key to be used when storing instances of the modeled type
     *
     * @param discriminatorKey the discriminator key value
     * @return this
     */
    public ClassModelBuilder<T> discriminatorKey(final String discriminatorKey) {
        this.discriminatorKey = discriminatorKey;
        return this;
    }

    /**
     * @return the discriminator key to be used when storing instances of the modeled type or null if not set
     */
    public String getDiscriminatorKey() {
        return discriminatorKey;
    }

    /**
     * Enables or disables the use of a discriminator when serializing
     *
     * @param discriminatorEnabled true to enable the use of a discriminator
     * @return this
     */
    public ClassModelBuilder<T> enableDiscriminator(final boolean discriminatorEnabled) {
        this.discriminatorEnabled = discriminatorEnabled;
        return this;
    }

    /**
     * @return true if a discriminator should be used when serializing, otherwise false
     */
    public Boolean useDiscriminator() {
        return discriminatorEnabled;
    }

    /**
     * Designates a property as the {@code _id} property for this type.  If another property is currently marked as the  {@code _id}
     * property, that setting is cleared in favor of the named property.
     *
     * @param idPropertyName the property name to use for the {@code _id} property, a null value removes the set idPropertyName.
     *
     * @return this
     */
    public ClassModelBuilder<T> idPropertyName(final String idPropertyName) {
        this.idPropertyName = idPropertyName;
        return this;
    }

    /**
     * @return the designated {@code _id} property name for this type or null if not set
     */
    public String getIdPropertyName() {
        return idPropertyName;
    }

    /**
     * Remove a property from the builder
     *
     * @param propertyName the actual property name in the POJO and not the {@code documentPropertyName}.
     * @return returns true if the property matched and was removed
     */
    public boolean removeProperty(final String propertyName) {
        return propertyModelBuilders.remove(getProperty(notNull("propertyName", propertyName)));
    }

    /**
     * Gets a property by the property name.
     *
     * @param propertyName the name of the property to find.
     * @return the property or null if the property is not found
     */
    public PropertyModelBuilder<?> getProperty(final String propertyName) {
        notNull("propertyName", propertyName);
        for (PropertyModelBuilder<?> propertyModelBuilder : propertyModelBuilders) {
            if (propertyModelBuilder.getName().equals(propertyName)) {
                return propertyModelBuilder;
            }
        }
        return null;
    }

    /**
     * @return the properties on the modeled type
     */
    public List<PropertyModelBuilder<?>> getPropertyModelBuilders() {
        return Collections.unmodifiableList(propertyModelBuilders);
    }

    /**
     * Creates a new ClassModel instance based on the mapping data provided.
     *
     * @return the new instance
     */
    public ClassModel<T> build() {
        List<PropertyModel<?>> propertyModels = new ArrayList<PropertyModel<?>>();
        PropertyModel<?> idPropertyModel = null;

        stateNotNull("type", type);
        for (Convention convention : conventions) {
            convention.apply(this);
        }

        stateNotNull("instanceCreatorFactory", instanceCreatorFactory);
        if (discriminatorEnabled) {
            stateNotNull("discriminatorKey", discriminatorKey);
            stateNotNull("discriminator", discriminator);
        }

        for (PropertyModelBuilder<?> propertyModelBuilder : propertyModelBuilders) {
            boolean isIdProperty = propertyModelBuilder.getName().equals(idPropertyName);
            if (isIdProperty) {
                propertyModelBuilder.readName(ID_PROPERTY_NAME).writeName(ID_PROPERTY_NAME);
            }

            PropertyModel<?> model = propertyModelBuilder.build();
            propertyModels.add(model);
            if (isIdProperty) {
                idPropertyModel = model;
            }
        }
        validatePropertyModels(type.getSimpleName(), propertyModels);
        return new ClassModel<T>(type, propertyNameToTypeParameterMap, instanceCreatorFactory, discriminatorEnabled, discriminatorKey,
                discriminator, IdPropertyModelHolder.create(type, idPropertyModel, idGenerator), unmodifiableList(propertyModels));
    }

    @Override
    public String toString() {
        return format("ClassModelBuilder{type=%s}", type);
    }

    Map<String, TypeParameterMap> getPropertyNameToTypeParameterMap() {
        return propertyNameToTypeParameterMap;
    }

    ClassModelBuilder<T> propertyNameToTypeParameterMap(final Map<String, TypeParameterMap> propertyNameToTypeParameterMap) {
        this.propertyNameToTypeParameterMap = unmodifiableMap(new HashMap<String, TypeParameterMap>(propertyNameToTypeParameterMap));
        return this;
    }

    ClassModelBuilder<T> addProperty(final PropertyModelBuilder<?> propertyModelBuilder) {
        propertyModelBuilders.add(notNull("propertyModelBuilder", propertyModelBuilder));
        return this;
    }

    private void validatePropertyModels(final String declaringClass, final List<PropertyModel<?>> propertyModels) {
        Map<String, Integer> propertyNameMap = new HashMap<String, Integer>();
        Map<String, Integer> propertyReadNameMap = new HashMap<String, Integer>();
        Map<String, Integer> propertyWriteNameMap = new HashMap<String, Integer>();

        for (PropertyModel<?> propertyModel : propertyModels) {
            checkForDuplicates("property", propertyModel.getName(), propertyNameMap, declaringClass);
            if (propertyModel.isReadable()) {
                checkForDuplicates("read property", propertyModel.getReadName(), propertyReadNameMap, declaringClass);
            }
            if (propertyModel.isWritable()) {
                checkForDuplicates("write property", propertyModel.getWriteName(), propertyWriteNameMap, declaringClass);
            }
        }

        if (idPropertyName != null && !propertyNameMap.containsKey(idPropertyName)) {
            throw new CodecConfigurationException(format("Invalid id property, property named '%s' can not be found.", idPropertyName));
        }
    }

    private void checkForDuplicates(final String propertyType, final String propertyName, final Map<String, Integer> propertyNameMap,
                                    final String declaringClass) {
        if (propertyNameMap.containsKey(propertyName)) {
            throw new CodecConfigurationException(format("Duplicate %s named '%s' found in %s.", propertyType, propertyName,
                    declaringClass));
        }
        propertyNameMap.put(propertyName, 1);
    }

}
