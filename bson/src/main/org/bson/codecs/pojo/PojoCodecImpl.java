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

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.lang.String.format;


final class PojoCodecImpl<T> extends PojoCodec<T> {
    private static final Logger LOGGER = Loggers.getLogger("PojoCodec");
    private static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();
    private final ClassModel<T> classModel;
    private final CodecRegistry registry;
    private final PropertyCodecRegistry propertyCodecRegistry;
    private final DiscriminatorLookup discriminatorLookup;

    PojoCodecImpl(final ClassModel<T> classModel, final CodecRegistry codecRegistry,
            final List<PropertyCodecProvider> propertyCodecProviders, final DiscriminatorLookup discriminatorLookup) {
        this.classModel = classModel;
        this.registry = codecRegistry;
        this.discriminatorLookup = discriminatorLookup;
        this.propertyCodecRegistry = new PropertyCodecRegistryImpl(this, registry, propertyCodecProviders);
        specialize();
    }

    PojoCodecImpl(final ClassModel<T> classModel, final CodecRegistry codecRegistry,
            final PropertyCodecRegistry propertyCodecRegistry, final DiscriminatorLookup discriminatorLookup) {
        this.classModel = classModel;
        this.registry = codecRegistry;
        this.discriminatorLookup = discriminatorLookup;
        this.propertyCodecRegistry = propertyCodecRegistry;
        specialize();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        if (areEquivalentTypes(value.getClass(), classModel.getType())) {
            writer.writeStartDocument();

            encodeIdProperty(writer, value, encoderContext, classModel.getIdPropertyModelHolder());

            if (classModel.useDiscriminator()) {
                writer.writeString(classModel.getDiscriminatorKey(), classModel.getDiscriminator());
            }

            for (PropertyModel<?> propertyModel : classModel.getPropertyModels()) {
                if (propertyModel.equals(classModel.getIdPropertyModel())) {
                    continue;
                }
                encodeProperty(writer, value, encoderContext, propertyModel);
            }
            writer.writeEndDocument();
        } else {
            ((Codec<T>) registry.get(value.getClass())).encode(writer, value, encoderContext);
        }
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        if (decoderContext.hasCheckedDiscriminator()) {
            InstanceCreator<T> instanceCreator = classModel.getInstanceCreator();
            decodeProperties(reader, decoderContext, instanceCreator);
            return instanceCreator.getInstance();
        } else {
            return getCodecFromDocument(reader, classModel.useDiscriminator(), classModel.getDiscriminatorKey(), registry,
                    discriminatorLookup, this, classModel.getName())
                    .decode(reader, DecoderContext.builder().checkedDiscriminator(true).build());
        }
    }

    @Override
    public Class<T> getEncoderClass() {
        return classModel.getType();
    }

    @Override
    public String toString() {
        return format("PojoCodec<%s>", classModel);
    }

    ClassModel<T> getClassModel() {
        return classModel;
    }

    private <S> void encodeIdProperty(final BsonWriter writer, final T instance, final EncoderContext encoderContext,
                                      final IdPropertyModelHolder<S> propertyModelHolder) {
        if (propertyModelHolder.getPropertyModel() != null) {
            if (propertyModelHolder.getIdGenerator() == null) {
                encodeProperty(writer, instance, encoderContext, propertyModelHolder.getPropertyModel());
            } else {
                S id = propertyModelHolder.getPropertyModel().getPropertyAccessor().get(instance);
                if (id == null && encoderContext.isEncodingCollectibleDocument()) {
                    id = propertyModelHolder.getIdGenerator().generate();
                    try {
                        propertyModelHolder.getPropertyModel().getPropertyAccessor().set(instance, id);
                    } catch (Exception e) {
                        // ignore
                    }
                }
                encodeValue(writer, encoderContext, propertyModelHolder.getPropertyModel(), id);
            }
        }
    }

    private <S> void encodeProperty(final BsonWriter writer, final T instance, final EncoderContext encoderContext,
                                    final PropertyModel<S> propertyModel) {
        if (propertyModel != null && propertyModel.isReadable()) {
            S propertyValue = propertyModel.getPropertyAccessor().get(instance);
            encodeValue(writer, encoderContext, propertyModel, propertyValue);
        }
    }

    @SuppressWarnings("unchecked")
    private <S> void encodeValue(final BsonWriter writer,  final EncoderContext encoderContext, final PropertyModel<S> propertyModel,
                                 final S propertyValue) {
        if (propertyModel.shouldSerialize(propertyValue)) {
            try {
                if (propertyModel.getPropertySerialization().inline()) {
                    if (propertyValue != null) {
                        new BsonDocumentWrapper<>(propertyValue, propertyModel.getCachedCodec()).forEach((k, v) -> {
                            writer.writeName(k);
                            encoderContext.encodeWithChildContext((Encoder<BsonValue>) registry.get(v.getClass()), writer, v);
                        });
                    }
                } else {
                    writer.writeName(propertyModel.getReadName());
                    if (propertyValue == null) {
                        writer.writeNull();
                    } else {
                        encoderContext.encodeWithChildContext(propertyModel.getCachedCodec(), writer, propertyValue);
                    }
                }
            } catch (CodecConfigurationException e) {
                throw new CodecConfigurationException(format("Failed to encode '%s'. Encoding '%s' errored with: %s",
                        classModel.getName(), propertyModel.getReadName(), e.getMessage()), e);
            }
        }
    }

    private void decodeProperties(final BsonReader reader, final DecoderContext decoderContext, final InstanceCreator<T> instanceCreator) {
        PropertyModel<?> inlineElementsPropertyModel = classModel.getPropertyModels()
                .stream()
                .filter(p -> p.getPropertySerialization().inline())
                .findFirst()
                .orElse(null);

        BsonDocument extraElements = inlineElementsPropertyModel  == null ? null : new BsonDocument();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (classModel.useDiscriminator() && classModel.getDiscriminatorKey().equals(name)) {
                reader.readString();
            } else {
                decodePropertyModel(reader, decoderContext, instanceCreator, name, getPropertyModelByWriteName(classModel, name), extraElements);
            }
        }
        reader.readEndDocument();
        setPropertyValueBsonExtraElements(instanceCreator, extraElements, inlineElementsPropertyModel);
    }

    private <S> void decodePropertyModel(final BsonReader reader, final DecoderContext decoderContext,
                                         final InstanceCreator<T> instanceCreator, final String name,
                                         final PropertyModel<S> propertyModel, @Nullable final BsonDocument extraElements) {
        if (propertyModel != null) {
            setPropertyValue(instanceCreator, () -> {
                S value = null;
                if (reader.getCurrentBsonType() == BsonType.NULL) {
                    reader.readNull();
                } else {
                    Codec<S> codec = propertyModel.getCachedCodec();
                    if (codec == null) {
                        throw new CodecConfigurationException(format("Missing codec in '%s' for '%s'",
                                classModel.getName(), propertyModel.getName()));
                    }
                    value = decoderContext.decodeWithChildContext(codec, reader);
                }
                return value;
            }, propertyModel);
        } else if (extraElements == null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Found property not present in the ClassModel: %s", name));
            }
            reader.skipValue();
        } else {
            try {
                extraElements.append(name, decoderContext.decodeWithChildContext(BSON_VALUE_CODEC, reader));
            } catch (CodecConfigurationException e) {
                throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding '%s' errored with: %s",
                        classModel.getName(), name, e.getMessage()), e);
            }
        }
    }

    private <S> void setPropertyValue(final InstanceCreator<T> instanceCreator, final Supplier<S> valueSupplier,
            final PropertyModel<S> propertyModel) {
        try {
            instanceCreator.set(valueSupplier.get(), propertyModel);
        } catch (BsonInvalidOperationException | CodecConfigurationException e) {
            throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding '%s' errored with: %s",
                    classModel.getName(), propertyModel.getName(), e.getMessage()), e);
        }
    }

    private <S> void setPropertyValueBsonExtraElements(final InstanceCreator<T> instanceCreator, @Nullable final BsonDocument extraElements,
             final PropertyModel<S> inlineElementsPropertyModel) {
        if (extraElements != null
                && !extraElements.isEmpty()
                && inlineElementsPropertyModel != null
                && inlineElementsPropertyModel.isWritable()) {
            setPropertyValue(instanceCreator, () ->
                    inlineElementsPropertyModel.getCachedCodec()
                            .decode(new BsonDocumentReader(extraElements), DecoderContext.builder().build()),
                    inlineElementsPropertyModel);
        }
    }

    private void specialize() {
        classModel.getPropertyModels().forEach(this::cachePropertyModelCodec);
    }

    private <S> void cachePropertyModelCodec(final PropertyModel<S> propertyModel) {
        if (propertyModel.getCachedCodec() == null) {
            Codec<S> codec = propertyModel.getCodec() != null ? propertyModel.getCodec()
                    : new LazyPropertyModelCodec<>(propertyModel, registry, propertyCodecRegistry);
            propertyModel.cachedCodec(codec);
        }
    }

    private <S, V> boolean areEquivalentTypes(final Class<S> t1, final Class<V> t2) {
        if (t1.equals(t2)) {
            return true;
        } else if (Collection.class.isAssignableFrom(t1) && Collection.class.isAssignableFrom(t2)) {
            return true;
        } else if (Map.class.isAssignableFrom(t1) && Map.class.isAssignableFrom(t2)) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    static <C> Codec<C> getCodecFromDocument(final BsonReader reader, final boolean useDiscriminator, final String discriminatorKey,
            final CodecRegistry registry, final DiscriminatorLookup discriminatorLookup, @Nullable final Codec<C> defaultCodec,
            final String simpleClassName) {
        Codec<C> codec = defaultCodec;
        if (useDiscriminator) {
            BsonReaderMark mark = reader.getMark();
            reader.readStartDocument();
            boolean discriminatorKeyFound = false;
            while (!discriminatorKeyFound && reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String name = reader.readName();
                if (discriminatorKey.equals(name)) {
                    discriminatorKeyFound = true;
                    try {
                        Class<?> discriminatorClass = discriminatorLookup.lookup(reader.readString());
                        if (codec == null || !codec.getEncoderClass().equals(discriminatorClass)) {
                            codec = (Codec<C>) registry.get(discriminatorClass);
                        }
                    } catch (Exception e) {
                        throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding errored with: %s",
                                simpleClassName, e.getMessage()), e);
                    }
                } else {
                    reader.skipValue();
                }
            }
            mark.reset();
        }
        return codec;
    }

    private PropertyModel<?> getPropertyModelByWriteName(final ClassModel<T> classModel, final String readName) {
        for (PropertyModel<?> propertyModel : classModel.getPropertyModels()) {
            if (propertyModel.isWritable() && propertyModel.getWriteName().equals(readName)) {
                return propertyModel;
            }
        }
        return null;
    }

    @Override
    DiscriminatorLookup getDiscriminatorLookup() {
        return discriminatorLookup;
    }
}
