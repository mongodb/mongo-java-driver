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

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;

import java.lang.annotation.Annotation;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.pojo.PojoBuilderHelper.stateNotNull;

/**
 * A builder for programmatically creating {@code PropertyModels}.
 *
 * @param <T> the type of the property
 * @since 3.5
 * @see PropertyModel
 */
public final class PropertyModelBuilder<T> {
    private String propertyName;
    private String documentPropertyName;
    private TypeData<T> typeData;
    private PropertySerialization<T> propertySerialization;
    private Codec<T> codec;
    private PropertyAccessor<T> propertyAccessor;
    private List<Annotation> annotations = emptyList();
    private Boolean discriminatorEnabled;

    PropertyModelBuilder() {
    }

    /**
     * @return the property name
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * @return the property name to be used when serializing the property
     */
    public String getDocumentPropertyName() {
        return documentPropertyName;
    }

    /**
     * Sets the document property name as it will be stored in the database.
     *
     * @param documentPropertyName the document property name
     * @return this
     */
    public PropertyModelBuilder<T> documentPropertyName(final String documentPropertyName) {
        this.documentPropertyName = notNull("documentPropertyName", documentPropertyName);
        return this;
    }

    /**
     * Sets a custom codec for the property
     *
     * @param codec the custom codec for the property
     * @return this
     */
    public PropertyModelBuilder<T> codec(final Codec<T> codec) {
        this.codec = codec;
        return this;
    }

    /**
     * @return the custom codec to use if set or null
     */
    Codec<T> getCodec() {
        return codec;
    }

    /**
     * Sets the {@link PropertySerialization} checker
     *
     * @param propertySerialization checks if a property should be serialized
     * @return this
     */
    public PropertyModelBuilder<T> propertySerialization(final PropertySerialization<T> propertySerialization) {
        this.propertySerialization = notNull("propertySerialization", propertySerialization);
        return this;
    }

    /**
     * @return the {@link PropertySerialization} checker
     */
    public PropertySerialization<T> getPropertySerialization() {
        return propertySerialization;
    }

    /**
     * Returns the annotations
     *
     * @return the annotations
     */
    public List<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * Sets the annotations
     *
     * @param annotations the annotations
     * @return this
     */
    public PropertyModelBuilder<T> annotations(final List<Annotation> annotations) {
        this.annotations = unmodifiableList(notNull("annotations", annotations));
        return this;
    }

    /**
     * @return true or false if a discriminator should be used when serializing or null if not set
     */
    public Boolean isDiscriminatorEnabled() {
        return discriminatorEnabled;
    }

    /**
     * Enables or disables the use of a discriminator when serializing
     *
     * @param discriminatorEnabled the useDiscriminator value
     * @return this
     */
    public PropertyModelBuilder<T> discriminatorEnabled(final boolean discriminatorEnabled) {
        this.discriminatorEnabled = discriminatorEnabled;
        return this;
    }

    /**
     * Returns the {@link PropertyAccessor}
     *
     * @return the PropertyAccessor
     */
    public PropertyAccessor<T> getPropertyAccessor() {
        return propertyAccessor;
    }

    /**
     * Sets the {@link PropertyAccessor}
     *
     * @param propertyAccessor the PropertyAccessor
     * @return this
     */
    public PropertyModelBuilder<T> propertyAccessor(final PropertyAccessor<T> propertyAccessor) {
        this.propertyAccessor = propertyAccessor;
        return this;
    }

    /**
     * Creates the {@link PropertyModel} from the {@link PropertyModelBuilder}.
     *
     * @return the PropertyModel
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public PropertyModel<T> build() {
        return new PropertyModel(
                stateNotNull("propertyName", propertyName),
                stateNotNull("documentPropertyName", documentPropertyName),
                stateNotNull("typeData", typeData),
                codec,
                stateNotNull("propertySerialization", propertySerialization),
                discriminatorEnabled,
                stateNotNull("propertyAccessor", propertyAccessor));
    }

    @Override
    public String toString() {
        return format("PropertyModelBuilder{propertyName=%s, typeData=%s}", propertyName, typeData);
    }

    PropertyModelBuilder<T> propertyName(final String propertyName) {
        this.propertyName = notNull("propertyName", propertyName);
        return this;
    }

    TypeData<T> getTypeData() {
        return typeData;
    }

    PropertyModelBuilder<T> typeData(final TypeData<T> typeData) {
        this.typeData = notNull("typeData", typeData);
        return this;
    }
}
