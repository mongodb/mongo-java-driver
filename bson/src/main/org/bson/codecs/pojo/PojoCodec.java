/*
 * Copyright 2017 MongoDB, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


final class PojoCodec<T> implements Codec<T> {
    private static final Logger LOGGER = Loggers.getLogger("PojoCodec");
    private final ClassModel<T> classModel;
    private final PojoCodecProvider codecProvider;
    private final CodecRegistry registry;
    private final DiscriminatorLookup discriminatorLookup;
    private final ConcurrentMap<ClassModel<?>, Codec<?>> codecCache;
    private final boolean specialized;


    PojoCodec(final ClassModel<T> classModel, final PojoCodecProvider codecProvider, final CodecRegistry registry,
              final DiscriminatorLookup discriminatorLookup) {
        this(classModel, codecProvider, registry, discriminatorLookup, new ConcurrentHashMap<ClassModel<?>, Codec<?>>(),
                !classModel.hasTypeParameters());
    }

    PojoCodec(final ClassModel<T> classModel, final PojoCodecProvider codecProvider, final CodecRegistry registry,
              final DiscriminatorLookup discriminatorLookup, final ConcurrentMap<ClassModel<?>, Codec<?>> codecCache,
              final boolean specialized) {
        this.classModel = classModel;
        this.codecProvider = codecProvider;
        this.registry = fromRegistries(fromCodecs(this), registry);
        this.discriminatorLookup = discriminatorLookup;
        this.codecCache = codecCache;
        this.specialized = specialized;
        if (specialized) {
            codecCache.put(classModel, this);
            for (PropertyModel<?> propertyModel : classModel.getPropertyModels()) {
                addToCache(propertyModel);
            }
        }
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        if (!specialized) {
            throw new CodecConfigurationException("Cannot encode an unspecialized generic ClassModel");
        }
        writer.writeStartDocument();
        PropertyModel<?> idPropertyModel = classModel.getIdPropertyModel();
        if (idPropertyModel != null) {
            encodeProperty(writer, value, encoderContext, idPropertyModel);
        }

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
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        if (decoderContext.hasCheckedDiscriminator()) {
            if (!specialized) {
                throw new CodecConfigurationException("Cannot decode using an unspecialized generic ClassModel");
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

    @SuppressWarnings("unchecked")
    private <S> void encodeProperty(final BsonWriter writer, final T instance, final EncoderContext encoderContext,
                                    final PropertyModel<S> propertyModel) {
        if (propertyModel.isReadable()) {
            S propertyValue = propertyModel.getPropertyAccessor().get(instance);
            if (propertyModel.shouldSerialize(propertyValue)) {
                writer.writeName(propertyModel.getReadName());
                if (propertyValue == null) {
                    writer.writeNull();
                } else {
                    getInstanceCodec(propertyModel, propertyValue.getClass()).encode(writer, propertyValue, encoderContext);
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
                    value = decoderContext.decodeWithChildContext(propertyModel.getCachedCodec(), reader);
                }
                if (propertyModel.isWritable()) {
                    instanceCreator.set(value, propertyModel);
                }
            } catch (BsonInvalidOperationException e) {
                throw new CodecConfigurationException(format("Failed to decode '%s'. %s", name, e.getMessage()), e);
            } catch (CodecConfigurationException e) {
                throw new CodecConfigurationException(format("Failed to decode '%s'. %s", name, e.getMessage()), e);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Found property not present in the ClassModel: %s", name));
            }
            reader.skipValue();
        }
    }

    private <S> void addToCache(final PropertyModel<S> propertyModel) {
        Codec<S> codec = propertyModel.getCodec() != null ? propertyModel.getCodec()
                : specializePojoCodec(propertyModel, getCodecFromTypeData(propertyModel.getTypeData()));
        propertyModel.cachedCodec(codec);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private  <S> Codec<S> getCodecFromTypeData(final TypeData<S> typeData) {
        Codec<S> codec = null;
        Class<S> head = typeData.getType();

        if (Collection.class.isAssignableFrom(head)) {
            codec = new CollectionCodec(head, getCodecFromTypeData(typeData.getTypeParameters().get(0)));
        } else if (Map.class.isAssignableFrom(head)) {
            codec = new MapCodec(head, getCodecFromTypeData(typeData.getTypeParameters().get(1)));
        } else if (Enum.class.isAssignableFrom(head)) {
            try {
                codec = registry.get(head);
            } catch (CodecConfigurationException e) {
                codec = new EnumCodec((Class<Enum<?>>) head);
            }
        } else {
            codec = getCodecFromClass(head);
        }


        return codec;
    }

    @SuppressWarnings("unchecked")
    private <S, V> Codec<S> getInstanceCodec(final PropertyModel<S> propertyModel, final Class<V> instanceType) {
        Codec<S> codec = propertyModel.getCachedCodec();
        if (!areEquivalentTypes(codec.getEncoderClass(), instanceType)) {
            codec = (Codec<S>) registry.get(instanceType);
        }
        return codec;
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
    private <S> Codec<S> getCodecFromClass(final Class<S> clazz) {
        Codec<S> codec = null;
        if (classModel.getType().equals(clazz)) {
            codec = (Codec<S>) this;
        } else {
            codec = codecProvider.getPojoCodec(clazz, registry);
        }
        if (codec == null) {
            codec = registry.get(clazz);
        }
        return codec;
    }

    @SuppressWarnings("unchecked")
    private <S> Codec<S> specializePojoCodec(final PropertyModel<S> propertyModel, final Codec<S> defaultCodec) {
        Codec<S> codec = defaultCodec;
        if (codec != null && codec instanceof PojoCodec) {
            PojoCodec<S> pojoCodec = (PojoCodec<S>) codec;
            ClassModel<S> specialized = getSpecializedClassModel(pojoCodec.getClassModel(), propertyModel);
            if (codecCache.containsKey(specialized)) {
                codec = (Codec<S>) codecCache.get(specialized);
            } else {
                codec = new LazyPojoCodec<S>(specialized, codecProvider, registry, discriminatorLookup, codecCache);
            }
        }
        return codec;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <S, V> ClassModel<S> getSpecializedClassModel(final ClassModel<S> clazzModel, final PropertyModel<V> propertyModel) {
        boolean useDiscriminator = propertyModel.useDiscriminator() == null ? clazzModel.useDiscriminator()
                : propertyModel.useDiscriminator();
        boolean validDiscriminator = clazzModel.getDiscriminatorKey() != null && clazzModel.getDiscriminator() != null;
        boolean changeTheDiscriminator = (useDiscriminator != clazzModel.useDiscriminator()) && validDiscriminator;

        if (propertyModel.getTypeData().getTypeParameters().isEmpty() && !changeTheDiscriminator){
            return clazzModel;
        }

        ArrayList<PropertyModel<?>> concretePropertyModels = new ArrayList<PropertyModel<?>>(clazzModel.getPropertyModels());
        PropertyModel<?> concreteIdProperty = clazzModel.getIdPropertyModel();

        List<TypeData<?>> propertyTypeParameters = propertyModel.getTypeData().getTypeParameters();
        for (int i = 0; i < concretePropertyModels.size(); i++) {
            PropertyModel<?> model = concretePropertyModels.get(i);
            String propertyName = model.getName();
            TypeParameterMap typeParameterMap = clazzModel.getPropertyNameToTypeParameterMap().get(propertyName);
            if (typeParameterMap.hasTypeParameters()) {
                PropertyModel<?> concretePropertyModel = getSpecializedPropertyModel(model, typeParameterMap, propertyTypeParameters);
                concretePropertyModels.set(i, concretePropertyModel);
                if (concreteIdProperty != null && concreteIdProperty.getName().equals(propertyName)) {
                    concreteIdProperty = concretePropertyModel;
                }
            }
        }

        boolean discriminatorEnabled = changeTheDiscriminator ? propertyModel.useDiscriminator() : clazzModel.useDiscriminator();
        return new ClassModel<S>(clazzModel.getType(), clazzModel.getPropertyNameToTypeParameterMap(),
                clazzModel.getInstanceCreatorFactory(), discriminatorEnabled, clazzModel.getDiscriminatorKey(),
                clazzModel.getDiscriminator(), concreteIdProperty, concretePropertyModels);
    }

    @SuppressWarnings("unchecked")
    private <V> PropertyModel<V> getSpecializedPropertyModel(final PropertyModel<V> propertyModel, final TypeParameterMap typeParameterMap,
                                                             final List<TypeData<?>> propertyTypeParameters) {
        TypeData<V> specializedPropertyType = propertyModel.getTypeData();
        Map<Integer, Integer> propertyToClassParamIndexMap = typeParameterMap.getPropertyToClassParamIndexMap();
        Integer classTypeParamRepresentsWholeProperty = propertyToClassParamIndexMap.get(-1);
        if (classTypeParamRepresentsWholeProperty != null) {
            specializedPropertyType = (TypeData<V>) propertyTypeParameters.get(classTypeParamRepresentsWholeProperty);
        } else {
            TypeData.Builder<V> builder = TypeData.builder(propertyModel.getTypeData().getType());
            List<TypeData<?>> typeParameters = new ArrayList<TypeData<?>>(propertyModel.getTypeData().getTypeParameters());
            for (int i = 0; i < typeParameters.size(); i++) {
                for (Map.Entry<Integer, Integer> mapping : propertyToClassParamIndexMap.entrySet()) {
                    if (mapping.getKey().equals(i)) {
                        typeParameters.set(i, propertyTypeParameters.get(mapping.getValue()));
                    }
                }
            }
            builder.addTypeParameters(typeParameters);
            specializedPropertyType = builder.build();
        }
        if (propertyModel.getTypeData().equals(specializedPropertyType)) {
            return propertyModel;
        }

        return new PropertyModel<V>(propertyModel.getName(), propertyModel.getReadName(), propertyModel.getWriteName(),
                specializedPropertyType, null, propertyModel.getPropertySerialization(), propertyModel.useDiscriminator(),
                propertyModel.getPropertyAccessor());
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
                    codec = (Codec<T>) registry.get(discriminatorLookup.lookup(reader.readString()));
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

}
