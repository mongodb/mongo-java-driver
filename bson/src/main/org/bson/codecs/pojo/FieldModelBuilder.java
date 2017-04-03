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
import java.lang.reflect.Field;
import java.util.List;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.pojo.PojoBuilderHelper.configureFieldModelBuilder;
import static org.bson.codecs.pojo.PojoBuilderHelper.stateNotNull;

/**
 * A builder for programmatically creating {@code FieldModels}.
 *
 * @param <T> the type of the field
 * @since 3.5
 * @see FieldModel
 */
public final class FieldModelBuilder<T> {
    private String fieldName;
    private String documentFieldName;
    private TypeData<T> typeData;
    private FieldSerialization<T> fieldSerialization;
    private Codec<T> codec;
    private FieldAccessor<T> fieldAccessor;
    private List<Annotation> annotations = emptyList();
    private Boolean discriminatorEnabled;

    FieldModelBuilder(final Field field) {
        configureFieldModelBuilder(this, field);
    }

    /**
     * @return the field name
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return the field name
     */
    public String getDocumentFieldName() {
        return documentFieldName;
    }

    /**
     * Sets the document field name as it will be stored in the database.
     *
     * @param documentFieldName the document field name
     * @return this
     */
    public FieldModelBuilder<T> documentFieldName(final String documentFieldName) {
        this.documentFieldName = notNull("documentFieldName", documentFieldName);
        return this;
    }

    /**
     * Sets a custom codec for the field
     *
     * @param codec the custom codec for the field
     * @return this
     */
    public FieldModelBuilder<T> codec(final Codec<T> codec) {
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
     * Sets the {@link FieldSerialization} checker
     *
     * @param fieldSerialization checks if a field should be serialized
     * @return this
     */
    public FieldModelBuilder<T> fieldSerialization(final FieldSerialization<T> fieldSerialization) {
        this.fieldSerialization = notNull("fieldSerialization", fieldSerialization);
        return this;
    }

    /**
     * @return the {@link FieldSerialization} checker
     */
    public FieldSerialization<T> getFieldSerialization() {
        return fieldSerialization;
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
    public FieldModelBuilder<T> annotations(final List<Annotation> annotations) {
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
    public FieldModelBuilder<T> discriminatorEnabled(final boolean discriminatorEnabled) {
        this.discriminatorEnabled = discriminatorEnabled;
        return this;
    }

    /**
     * Returns the fieldAccessor
     *
     * @return the fieldAccessor
     */
    public FieldAccessor<T> getFieldAccessor() {
        return fieldAccessor;
    }

    /**
     * Sets the fieldAccessor
     *
     * @param fieldAccessor the fieldAccessor
     * @return this
     */
    public FieldModelBuilder<T> fieldAccessor(final FieldAccessor<T> fieldAccessor) {
        this.fieldAccessor = fieldAccessor;
        return this;
    }

    /**
     * Creates the FieldModel from the FieldModelBuilder.
     * @return the fieldModel
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FieldModel<T> build() {
        return new FieldModel(
                stateNotNull("fieldName", fieldName),
                stateNotNull("documentFieldName", documentFieldName),
                stateNotNull("typeData", typeData),
                codec,
                stateNotNull("fieldModelSerialization", fieldSerialization),
                discriminatorEnabled,
                stateNotNull("fieldAccessor", fieldAccessor));
    }

    @Override
    public String toString() {
        return format("FieldModelBuilder{fieldName=%s, typeData=%s}", fieldName, typeData);
    }

    FieldModelBuilder<T> fieldName(final String fieldName) {
        this.fieldName = notNull("fieldName", fieldName);
        return this;
    }

    TypeData<T> getTypeData() {
        return typeData;
    }

    FieldModelBuilder<T> typeData(final TypeData<T> typeData) {
        this.typeData = notNull("typeData", typeData);
        return this;
    }
}
