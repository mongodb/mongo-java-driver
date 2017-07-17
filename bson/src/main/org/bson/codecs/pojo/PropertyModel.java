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

/**
 * Represents a property on a class and stores various metadata such as generic parameters
 *
 * @param <T> the type of the property that the PropertyModel represents.
 * @since 3.5
 */
public final class PropertyModel<T> {
    private final String propertyName;
    private final String documentPropertyName;
    private final TypeData<T> typeData;
    private final Codec<T> codec;
    private final PropertySerialization<T> propertySerialization;
    private final Boolean useDiscriminator;
    private final PropertyAccessor<T> propertyAccessor;
    private volatile Codec<T> cachedCodec;

    PropertyModel(final String propertyName, final String documentPropertyName, final TypeData<T> typeData, final Codec<T> codec,
                  final PropertySerialization<T> propertySerialization, final Boolean useDiscriminator,
                  final PropertyAccessor<T> propertyAccessor) {
        this.propertyName = propertyName;
        this.documentPropertyName = documentPropertyName;
        this.typeData = typeData;
        this.codec = codec;
        this.cachedCodec = codec;
        this.propertySerialization = propertySerialization;
        this.useDiscriminator = useDiscriminator;
        this.propertyAccessor = propertyAccessor;
    }

    /**
     * Create a new {@link PropertyModelBuilder}
     * @param <T> the type of the property
     * @return the builder
     */
    public static <T> PropertyModelBuilder<T> builder() {
        return new PropertyModelBuilder<T>();
    }


    /**
     * Returns true if the value should be serialized.
     *
     * @param value the value to check
     * @return true if the value should be serialized.
     */
    public boolean shouldSerialize(final T value) {
        return propertySerialization.shouldSerialize(value);
    }

    /**
     * @return the property accessor
     */
    public PropertyAccessor<T> getPropertyAccessor() {
        return propertyAccessor;
    }

    /**
     * @return the unmapped property name as defined in the source file.
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * @return the name of the mapped property
     */
    public String getDocumentPropertyName() {
        return documentPropertyName;
    }

    /**
     * @return the type data for the property
     */
    public TypeData<T> getTypeData() {
        return typeData;
    }

    /**
     * @return the custom codec to use if set or null
     */
    public Codec<T> getCodec() {
        return codec;
    }

    /**
     * @return true or false if a discriminator should be used when serializing or null if not set
     */
    public Boolean useDiscriminator() {
        return useDiscriminator;
    }

    @Override
    public String toString() {
        return "PropertyModel{"
                + "propertyName='" + propertyName + "'"
                + ", documentPropertyName='" + documentPropertyName + "'"
                + ", typeData=" + typeData
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PropertyModel)) {
            return false;
        }

        PropertyModel<?> that = (PropertyModel<?>) o;

        if (useDiscriminator() != null ? !useDiscriminator().equals(that.useDiscriminator()) : that.useDiscriminator() != null) {
            return false;
        }

        if (getPropertyName() != null ? !getPropertyName().equals(that.getPropertyName()) : that.getPropertyName() != null) {
            return false;
        }
        if (getDocumentPropertyName() != null ? !getDocumentPropertyName().equals(that.getDocumentPropertyName())
                : that.getDocumentPropertyName() != null) {
            return false;
        }
        if (getTypeData() != null ? !getTypeData().equals(that.getTypeData()) : that.getTypeData() != null) {
            return false;
        }
        if (getCodec() != null ? !getCodec().equals(that.getCodec()) : that.getCodec() != null) {
            return false;
        }
        if (getPropertySerialization() != null ? !getPropertySerialization().equals(that.getPropertySerialization())
                : that.getPropertySerialization() != null) {
            return false;
        }
        if (getPropertyAccessor() != null ? !getPropertyAccessor().equals(that.getPropertyAccessor())
                : that.getPropertyAccessor() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getPropertyName() != null ? getPropertyName().hashCode() : 0;
        result = 31 * result + (getDocumentPropertyName() != null ? getDocumentPropertyName().hashCode() : 0);
        result = 31 * result + (getTypeData() != null ? getTypeData().hashCode() : 0);
        result = 31 * result + (getCodec() != null ? getCodec().hashCode() : 0);
        result = 31 * result + (getPropertySerialization() != null ? getPropertySerialization().hashCode() : 0);
        result = 31 * result + (useDiscriminator != null ? useDiscriminator.hashCode() : 0);
        result = 31 * result + (getPropertyAccessor() != null ? getPropertyAccessor().hashCode() : 0);
        return result;
    }

    PropertySerialization<T> getPropertySerialization() {
        return propertySerialization;
    }

    void cachedCodec(final Codec<T> codec) {
        this.cachedCodec = codec;
    }

    Codec<T> getCachedCodec() {
        return cachedCodec;
    }
}
