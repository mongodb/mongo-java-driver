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
package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.members.ResolvedField;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.mapper.conventions.Converter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Represents a field on a class and stores various metadata such as generic parameters for use by the {@link ClassModelCodec}
 */
@SuppressWarnings("unchecked")
public final class FieldModel extends MappedType {
    private final Field rawField;

    private final WeightedValue<String> name;
    private final WeightedValue<Boolean> included = new WeightedValue<Boolean>(true);
    private final WeightedValue<Boolean> storeNulls = new WeightedValue<Boolean>(true);
    private final WeightedValue<Boolean> storeEmpties = new WeightedValue<Boolean>(true);
    private final ClassModel owner;
    private final CodecRegistry registry;
    private final ResolvedField field;
    private Codec<?> codec;
    private Converter<?, ?> converter = new IdentityConverter();

    /**
     * Create the FieldModel
     *
     * @param classModel the owning ClassModel
     * @param registry   the TypeRegistry used to cache type information
     * @param field      the field to model
     */
    public FieldModel(final ClassModel classModel, final CodecRegistry registry, final ResolvedField field) {
        super(field.getType().getErasedType());
        owner = classModel;
        this.registry = registry;
        this.field = field;
        rawField = field.getRawMember();
        rawField.setAccessible(true);
        name = new WeightedValue<String>(field.getName());

        final List<ResolvedType> typeParameters = field.getType().getTypeParameters();
        for (final ResolvedType parameter : typeParameters) {
            addParameter(parameter.getErasedType());
        }
    }

    /**
     * Creates a FieldModel based on an existing FieldModel
     *
     * @param type       the type of this new FieldModel.  e.g., when encrypting this might be a byte[] rather than the original field's
     *                   String type
     * @param fieldModel the model to duplicate
     */
    public FieldModel(final Class<?> type, final FieldModel fieldModel) {
        super(type);
        this.registry = fieldModel.registry;
        this.name = fieldModel.name;
        this.owner = fieldModel.owner;
        this.field = fieldModel.field;
        this.rawField = null;
    }

    /**
     * Sets the field on entity with the given value
     *
     * @param entity  the entity to update
     * @param reader  The BsonReader to use
     * @param context the DecoderContext to use
     */
    public void decode(final Object entity, final BsonReader reader, final DecoderContext context) {
        try {
            rawField.set(entity, getConverter().unapply(getCodec().decode(reader, context)));
        } catch (final IllegalAccessException e) {
            // shouldn't get this but just in case...
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @return the Codec for this FieldModel
     */
    Codec<?> getCodec() {
        if (codec == null) {
            codec = registry.get(getConverter().getType());
        }
        return codec;
    }

    /**
     * Explicity sets the Codec to use for this FieldModel.  If not explicitly set, the Codec is looked up via the CodecRegistry.
     *
     * @param codec the code to use
     */
    public void setCodec(final Codec<?> codec) {
        this.codec = codec;
    }

    /**
     * @return returns the Converter for this field.
     */
    @SuppressWarnings("rawtypes")
    public Converter getConverter() {
        return converter;
    }

    /**
     * @param converter the Converter to use for this field.  Can not be null.
     */
    @SuppressWarnings("rawtypes")
    public void setConverter(final Converter converter) {
        if (converter == null) {
            throw new IllegalArgumentException("The converter can not be null");
        }
        this.converter = converter;
    }

    /**
     * Gives this field a chance to store itself in to the BsonWriter given.  If this field has been excluded by a Convention, this method
     * will do nothing.
     *
     * @param entity         the entity from which to pull the value this FieldModel represents
     * @param writer         the BsonWriter to use if this field is included
     * @param encoderContext the encoding context
     */
    public void encode(final Object entity, final BsonWriter writer, final EncoderContext encoderContext) {
        if (isIncluded()) {
            final Object value = get(entity);
            boolean toStore = value != null || storeNulls.get();
            if (Collection.class.isAssignableFrom(rawField.getType())) {
                toStore &= (!((Collection<?>) value).isEmpty() || storeEmpties.get());
            }
            if (Map.class.isAssignableFrom(rawField.getType())) {
                toStore &= (!((Map<?, ?>) value).isEmpty() || storeEmpties.get());
            }
            if (toStore) {
                writer.writeName(getName());
                final Codec<Object> codec = (Codec<Object>) getCodec();
                codec.encode(writer, getConverter().apply(value), encoderContext);
            }
        }
    }

    /**
     * @return true if the field should included
     */
    public Boolean isIncluded() {
        return included.get();
    }

    /**
     * Gets the value of the field from the given reference.
     *
     * @param entity the entity from which to pull the value
     * @return the value of the field
     */
    public Object get(final Object entity) {
        try {
            return rawField.get(entity);
        } catch (final IllegalAccessException e) {
            // shouldn't get this but just in case...
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @return the name of the mapped field
     */
    public String getName() {
        return name.get();
    }

    /**
     * @return the unmapped field name as defined in the java source.
     */
    public String getFieldName() {
        return rawField.getName();
    }

    /**
     * @return the Java field backing this FieldModel.  May be null in synthetic FieldModels.
     */
    public Field getRawField() {
        return rawField;
    }

    /**
     * Checks if this field is annotated with the given annotation.
     *
     * @param aClass the annotation to check for
     * @return true if the field is annotated with this type
     */
    public boolean hasAnnotation(final Class<? extends Annotation> aClass) {
        return field.getAnnotations().get(aClass) != null;
    }

    /**
     * @return true if the field is final
     */
    public boolean isFinal() {
        return field.isFinal();
    }

    /**
     * @return true if the field is private
     */
    public boolean isPrivate() {
        return field.isPrivate();
    }

    /**
     * @return true if the field is protected
     */
    public boolean isProtected() {
        return field.isProtected();
    }

    /**
     * @return true if the field is public
     */
    public boolean isPublic() {
        return field.isPublic();
    }

    /**
     * @return true if the field is static
     */
    public boolean isStatic() {
        return field.isStatic();
    }

    /**
     * @return true if the field is transient
     */
    public boolean isTransient() {
        return field.isTransient();
    }

    /**
     * Sets whether this field is to be included when de/encoding BSON documents.  Conventions are free to turn this field "off" or "on"
     * based on whatever criteria they need.  e.g., {@link org.bson.codecs.configuration.mapper.conventions.BeanPropertiesConvention} can
     * set this to false if a field isn't an instance field with proper set and get methods.
     *
     * @param weight  The weight to give to a particular suggested inclusion value
     * @param include Whether to include this field in processing or not
     */
    public void setIncluded(final int weight, final boolean include) {
        this.included.set(weight, include);
    }

    /**
     * Suggests a value for the name with a particular weight.
     *
     * @param weight the weight of the suggested value
     * @param value  the suggested value
     */
    public void setName(final Integer weight, final String value) {
        name.set(weight, value);
    }

    @Override
    public String toString() {
        return String.format("%s#%s", owner.getName(), field.getName());
    }

    private class IdentityConverter implements Converter<Object, Object> {
        @Override
        public Object apply(final Object value) {
            return value;
        }

        @Override
        public Class<Object> getType() {
            return (Class<Object>) getRawField().getType();
        }

        @Override
        public Object unapply(final Object value) {
            return value;
        }
    }
}
