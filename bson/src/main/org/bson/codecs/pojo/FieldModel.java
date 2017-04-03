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

import java.lang.reflect.Field;

/**
 * Represents a field on a class and stores various metadata such as generic parameters
 *
 * @param <T> the type of the field that the FieldModel represents.
 * @since 3.5
 */
public final class FieldModel<T> {
    private final String fieldName;
    private final String documentFieldName;
    private final TypeData<T> typeData;
    private final Codec<T> codec;
    private final FieldSerialization<T> fieldSerialization;
    private final Boolean useDiscriminator;
    private final FieldAccessor<T> fieldAccessor;
    private volatile Codec<T> cachedCodec;

    FieldModel(final String fieldName, final String documentFieldName, final TypeData<T> typeData, final Codec<T> codec,
               final FieldSerialization<T> fieldSerialization, final Boolean useDiscriminator,
               final FieldAccessor<T> fieldAccessor) {
        this.fieldName = fieldName;
        this.documentFieldName = documentFieldName;
        this.typeData = typeData;
        this.codec = codec;
        this.cachedCodec = codec;
        this.fieldSerialization = fieldSerialization;
        this.useDiscriminator = useDiscriminator;
        this.fieldAccessor = fieldAccessor;
    }

    static <S> FieldModelBuilder<S> builder(final Field field) {
        return new FieldModelBuilder<S>(field);
    }

    /**
     * Returns true if the value should be serialized.
     *
     * @param value the value to check
     * @return true if the value should be serialized.
     */
    public boolean shouldSerialize(final T value) {
        return fieldSerialization.shouldSerialize(value);
    }

    /**
     * @return the field accessor
     */
    public FieldAccessor<T> getFieldAccessor() {
        return fieldAccessor;
    }

    /**
     * @return the unmapped field name as defined in the source file.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return the name of the mapped field
     */
    public String getDocumentFieldName() {
        return documentFieldName;
    }

    /**
     * @return the type data for the field
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
        return "FieldModel{"
                + "fieldName='" + fieldName + "'"
                + ", documentFieldName='" + documentFieldName + "'"
                + ", typeData=" + typeData
                + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FieldModel)) {
            return false;
        }

        FieldModel<?> that = (FieldModel<?>) o;

        if (useDiscriminator() != null ? !useDiscriminator().equals(that.useDiscriminator()) : that.useDiscriminator() != null) {
            return false;
        }

        if (getFieldName() != null ? !getFieldName().equals(that.getFieldName()) : that.getFieldName() != null) {
            return false;
        }
        if (getDocumentFieldName() != null ? !getDocumentFieldName().equals(that.getDocumentFieldName())
                : that.getDocumentFieldName() != null) {
            return false;
        }
        if (getTypeData() != null ? !getTypeData().equals(that.getTypeData()) : that.getTypeData() != null) {
            return false;
        }
        if (getCodec() != null ? !getCodec().equals(that.getCodec()) : that.getCodec() != null) {
            return false;
        }
        if (getFieldSerialization() != null ? !getFieldSerialization().equals(that.getFieldSerialization())
                : that.getFieldSerialization() != null) {
            return false;
        }
        if (getFieldAccessor() != null ? !getFieldAccessor().equals(that.getFieldAccessor()) : that.getFieldAccessor() != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getFieldName() != null ? getFieldName().hashCode() : 0;
        result = 31 * result + (getDocumentFieldName() != null ? getDocumentFieldName().hashCode() : 0);
        result = 31 * result + (getTypeData() != null ? getTypeData().hashCode() : 0);
        result = 31 * result + (getCodec() != null ? getCodec().hashCode() : 0);
        result = 31 * result + (getFieldSerialization() != null ? getFieldSerialization().hashCode() : 0);
        result = 31 * result + (useDiscriminator != null ? useDiscriminator.hashCode() : 0);
        result = 31 * result + (getFieldAccessor() != null ? getFieldAccessor().hashCode() : 0);
        return result;
    }

    FieldSerialization<T> getFieldSerialization() {
        return fieldSerialization;
    }

    void cachedCodec(final Codec<T> codec) {
        this.cachedCodec = codec;
    }

    Codec<T> getCachedCodec() {
        return cachedCodec;
    }
}
