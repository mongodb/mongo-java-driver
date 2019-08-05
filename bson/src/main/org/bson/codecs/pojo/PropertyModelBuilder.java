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
    private String name;
    private String readName;
    private String writeName;
    private TypeData<T> typeData;
    private PropertySerialization<T> propertySerialization;
    private Codec<T> codec;
    private PropertyAccessor<T> propertyAccessor;
    private List<Annotation> readAnnotations = emptyList();
    private List<Annotation> writeAnnotations = emptyList();
    private Boolean discriminatorEnabled;
    private String error;

    PropertyModelBuilder() {
    }

    /**
     * @return the property name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the name of the property to use as the key when deserializing the data from BSON.
     */
    public String getReadName() {
        return readName;
    }

    /**
     * Sets the readName, the key for this property when deserializing the data from BSON.
     *
     * <p>Note: A null means this property will not used when deserializing.</p>
     *
     * @param readName the name of the property to use as the key when deserializing the data from BSON.
     * @return this
     */
    public PropertyModelBuilder<T> readName(final String readName) {
        this.readName = readName;
        return this;
    }

    /**
     * @return the name of the property to use as the key when serializing the data into BSON.
     */
    public String getWriteName() {
        return writeName;
    }

    /**
     * Sets the writeName, the key for this property when serializing the data into BSON.
     *
     * <p>Note: A null means this property will not be serialized.</p>
     *
     * @param writeName the name of the property to use as the key when serializing the data into BSON.
     * @return this
     */
    public PropertyModelBuilder<T> writeName(final String writeName) {
        this.writeName = writeName;
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
     * Returns the read annotations,  to be applied when serializing to BSON
     *
     * @return the read annotations
     */
    public List<Annotation> getReadAnnotations() {
        return readAnnotations;
    }

    /**
     * Sets the read annotations, to be applied when serializing to BSON
     *
     * @param annotations the read annotations
     * @return this
     */
    public PropertyModelBuilder<T> readAnnotations(final List<Annotation> annotations) {
        this.readAnnotations = unmodifiableList(notNull("annotations", annotations));
        return this;
    }

    /**
     * Returns the write annotations, to be applied when deserializing from BSON
     *
     * @return the write annotations
     */
    public List<Annotation> getWriteAnnotations() {
        return writeAnnotations;
    }

    /**
     * Sets the writeAnnotations, to be applied when deserializing from BSON
     *
     * @param writeAnnotations the writeAnnotations
     * @return this
     */
    public PropertyModelBuilder<T> writeAnnotations(final List<Annotation> writeAnnotations) {
        this.writeAnnotations = writeAnnotations;
        return this;
    }

    /**
     * Property is writable.
     *
     * @return true if can be deserialized from BSON
     */
    public boolean isWritable() {
        return writeName != null;
    }

    /**
     * Property is readable.
     *
     * @return true if can be serialized to BSON
     */
    public boolean isReadable() {
        return readName != null;
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
     * Creates the {@link PropertyModel}.
     *
     * @return the PropertyModel
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public PropertyModel<T> build() {
        if (!isReadable() && !isWritable()) {
            throw new IllegalStateException(format("Invalid PropertyModel '%s', neither readable or writable,", name));
        }
        return new PropertyModel(
                stateNotNull("propertyName", name),
                readName,
                writeName,
                stateNotNull("typeData", typeData),
                codec,
                stateNotNull("propertySerialization", propertySerialization),
                discriminatorEnabled,
                stateNotNull("propertyAccessor", propertyAccessor),
                error);
    }

    @Override
    public String toString() {
        return format("PropertyModelBuilder{propertyName=%s, typeData=%s}", name, typeData);
    }

    PropertyModelBuilder<T> propertyName(final String propertyName) {
        this.name = notNull("propertyName", propertyName);
        return this;
    }

    TypeData<T> getTypeData() {
        return typeData;
    }

    PropertyModelBuilder<T> typeData(final TypeData<T> typeData) {
        this.typeData = notNull("typeData", typeData);
        return this;
    }

    PropertyModelBuilder<T> setError(final String error) {
        this.error = error;
        return this;
    }
}
