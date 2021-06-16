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

import org.bson.BsonInvalidOperationException;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;


final class PojoCodecImpl<T> extends PojoCodec<T> {
    private static final Logger LOGGER = Loggers.getLogger("PojoCodec");
    private final ClassModel<T> classModel;
    private final CodecRegistry registry;
    private final PropertyCodecRegistry propertyCodecRegistry;
    private final DiscriminatorLookup discriminatorLookup;
    private final boolean specialized;

    PojoCodecImpl(final ClassModel<T> classModel, final CodecRegistry codecRegistry,
                  final List<PropertyCodecProvider> propertyCodecProviders, final DiscriminatorLookup discriminatorLookup) {
        this.classModel = classModel;
        this.registry = codecRegistry;
        this.discriminatorLookup = discriminatorLookup;
        this.propertyCodecRegistry = new PropertyCodecRegistryImpl(this, registry, propertyCodecProviders);
        this.specialized = shouldSpecialize(classModel);
        specialize();
    }

    PojoCodecImpl(final ClassModel<T> classModel, final CodecRegistry codecRegistry, final PropertyCodecRegistry propertyCodecRegistry,
                  final DiscriminatorLookup discriminatorLookup, final boolean specialized) {
        this.classModel = classModel;
        this.registry = codecRegistry;
        this.discriminatorLookup = discriminatorLookup;
        this.propertyCodecRegistry = propertyCodecRegistry;
        this.specialized = specialized;
        specialize();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        if (!specialized) {
            throw new CodecConfigurationException(format("%s contains generic types that have not been specialised.%n"
                            + "Top level classes with generic types are not supported by the PojoCodec.", classModel.getName()));
        }

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
            if (!specialized) {
                throw new CodecConfigurationException(format("%s contains generic types that have not been specialised.%n"
                        + "Top level classes with generic types are not supported by the PojoCodec.", classModel.getName()));
            }
            InstanceCreator<T> instanceCreator = classModel.getInstanceCreator();
            decodeProperties(reader, decoderContext, instanceCreator);
            return instanceCreator.getInstance();
        } else {
            return getCodecFromDocument(reader, classModel.useDiscriminator(), classModel.getDiscriminatorKey(), registry,
                    discriminatorLookup, this).decode(reader, DecoderContext.builder().checkedDiscriminator(true).build());
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

    @SuppressWarnings("unchecked")
    private <S> void encodeProperty(final BsonWriter writer, final T instance, final EncoderContext encoderContext,
                                    final PropertyModel<S> propertyModel) {
        if (propertyModel != null && propertyModel.isReadable()) {
            S propertyValue = propertyModel.getPropertyAccessor().get(instance);
            encodeValue(writer, encoderContext, propertyModel, propertyValue);
        }
    }

    private <S> void encodeValue(final BsonWriter writer,  final EncoderContext encoderContext, final PropertyModel<S> propertyModel,
                                 final S propertyValue) {
        if (propertyModel.shouldSerialize(propertyValue)) {
            writer.writeName(propertyModel.getReadName());
            if (propertyValue == null) {
                writer.writeNull();
            } else {
                try {
                    encoderContext.encodeWithChildContext(propertyModel.getCachedCodec(), writer, propertyValue);
                } catch (CodecConfigurationException e) {
                    throw new CodecConfigurationException(format("Failed to encode '%s'. Encoding '%s' errored with: %s",
                            classModel.getName(), propertyModel.getReadName(), e.getMessage()), e);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void decodeProperties(final BsonReader reader, final DecoderContext decoderContext, final InstanceCreator<T> instanceCreator) {
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String name = reader.readName();
            if (classModel.useDiscriminator() && classModel.getDiscriminatorKey().equals(name)) {
                reader.readString();
            } else {
                decodePropertyModel(reader, decoderContext, instanceCreator, name, getPropertyModelByWriteName(classModel, name));
            }
        }
        reader.readEndDocument();
    }

    @SuppressWarnings("unchecked")
    private <S> void decodePropertyModel(final BsonReader reader, final DecoderContext decoderContext,
                                         final InstanceCreator<T> instanceCreator, final String name,
                                         final PropertyModel<S> propertyModel) {
        if (propertyModel != null) {
            try {
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
                if (propertyModel.isWritable()) {
                    instanceCreator.set(value, propertyModel);
                }
            } catch (BsonInvalidOperationException | CodecConfigurationException e) {
                throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding '%s' errored with: %s",
                        classModel.getName(), name, e.getMessage()), e);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Found property not present in the ClassModel: %s", name));
            }
            reader.skipValue();
        }
    }

    private void specialize() {
        if (specialized) {
            classModel.getPropertyModels().forEach(this::cachePropertyModelCodec);
        }
    }

    private <S> void cachePropertyModelCodec(final PropertyModel<S> propertyModel) {
        if (propertyModel.getCachedCodec() == null) {
            Codec<S> codec = propertyModel.getCodec() != null ? propertyModel.getCodec()
                    : new LazyPropertyModelCodec<>(propertyModel, registry, propertyCodecRegistry, discriminatorLookup);
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
    private Codec<T> getCodecFromDocument(final BsonReader reader, final boolean useDiscriminator, final String discriminatorKey,
                                          final CodecRegistry registry, final DiscriminatorLookup discriminatorLookup,
                                          final Codec<T> defaultCodec) {
        Codec<T> codec = defaultCodec;
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
                        if (!codec.getEncoderClass().equals(discriminatorClass)) {
                            codec = (Codec<T>) registry.get(discriminatorClass);
                        }
                    } catch (Exception e) {
                        throw new CodecConfigurationException(format("Failed to decode '%s'. Decoding errored with: %s",
                                classModel.getName(), e.getMessage()), e);
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

    private static <T> boolean shouldSpecialize(final ClassModel<T> classModel) {
        if (!classModel.hasTypeParameters()) {
            return true;
        }

        for (Map.Entry<String, TypeParameterMap> entry : classModel.getPropertyNameToTypeParameterMap().entrySet()) {
            TypeParameterMap typeParameterMap = entry.getValue();
            PropertyModel<?> propertyModel = classModel.getPropertyModel(entry.getKey());
            if (typeParameterMap.hasTypeParameters() && (propertyModel == null || propertyModel.getCodec() == null)) {
                return false;
            }
        }
        return true;
    }

    @Override
    DiscriminatorLookup getDiscriminatorLookup() {
        return discriminatorLookup;
    }
}
