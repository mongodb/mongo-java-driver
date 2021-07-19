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

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.RepresentationConfigurable;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.pojo.PojoSpecializationHelper.specializeTypeData;

class LazyPropertyModelCodec<T> implements Codec<T> {
    private final PropertyModel<T> propertyModel;
    private final CodecRegistry registry;
    private final PropertyCodecRegistry propertyCodecRegistry;
    private final DiscriminatorLookup discriminatorLookup;

    private volatile Codec<T> codec;

    LazyPropertyModelCodec(final PropertyModel<T> propertyModel, final CodecRegistry registry,
                                  final PropertyCodecRegistry propertyCodecRegistry, final DiscriminatorLookup discriminatorLookup) {
        this.propertyModel = propertyModel;
        this.registry = registry;
        this.propertyCodecRegistry = propertyCodecRegistry;
        this.discriminatorLookup = discriminatorLookup;
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        return getPropertyModelCodec().decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        getPropertyModelCodec().encode(writer, value, encoderContext);
    }

    @Override
    public Class<T> getEncoderClass() {
        return propertyModel.getTypeData().getType();
    }

    private Codec<T> getPropertyModelCodec() {
        if (codec == null) {
            synchronized (this) {
                if (codec == null) {
                    codec = createCodec();
                }
            }
        }
        return codec;
    }

    private Codec<T> createCodec() {
        Codec<T> localCodec = getCodecFromPropertyRegistry(propertyModel);
        if (localCodec instanceof PojoCodec) {
            PojoCodec<T> pojoCodec = (PojoCodec<T>) localCodec;
            ClassModel<T> specialized = getSpecializedClassModel(pojoCodec.getClassModel(), propertyModel);
            localCodec = new PojoCodecImpl<>(specialized, registry, propertyCodecRegistry, pojoCodec.getDiscriminatorLookup(), true);
        }
        return localCodec;
    }

    @SuppressWarnings("unchecked")
    private Codec<T> getCodecFromPropertyRegistry(final PropertyModel<T> propertyModel) {
        Codec<T> localCodec;
        try {
            localCodec = propertyCodecRegistry.get(propertyModel.getTypeData());
        } catch (CodecConfigurationException e) {
            return new LazyMissingCodec<>(propertyModel.getTypeData().getType(), e);
        }
        if (localCodec == null) {
            localCodec = new LazyMissingCodec<>(propertyModel.getTypeData().getType(),
                    new CodecConfigurationException("Unexpected missing codec for: " + propertyModel.getName()));
        }
        BsonType representation = propertyModel.getBsonRepresentation();
        if (representation != null) {
            if (localCodec instanceof RepresentationConfigurable) {
                return ((RepresentationConfigurable<T>) localCodec).withRepresentation(representation);
            }
            throw new CodecConfigurationException("Codec must implement RepresentationConfigurable to support BsonRepresentation");
        }
        return localCodec;
    }

    private <V> ClassModel<T> getSpecializedClassModel(final ClassModel<T> clazzModel, final PropertyModel<V> propertyModel) {
        boolean useDiscriminator = propertyModel.useDiscriminator() == null ? clazzModel.useDiscriminator()
                : propertyModel.useDiscriminator();
        boolean validDiscriminator = clazzModel.getDiscriminatorKey() != null && clazzModel.getDiscriminator() != null;
        boolean changeTheDiscriminator = (useDiscriminator != clazzModel.useDiscriminator()) && validDiscriminator;

        if (propertyModel.getTypeData().getTypeParameters().isEmpty() && !changeTheDiscriminator){
            return clazzModel;
        }

        ArrayList<PropertyModel<?>> concretePropertyModels = new ArrayList<>(clazzModel.getPropertyModels());
        PropertyModel<?> concreteIdProperty = clazzModel.getIdPropertyModel();

        List<TypeData<?>> propertyTypeParameters = propertyModel.getTypeData().getTypeParameters();
        for (int i = 0; i < concretePropertyModels.size(); i++) {
            PropertyModel<?> model = concretePropertyModels.get(i);
            String propertyName = model.getName();
            TypeParameterMap typeParameterMap = clazzModel.getPropertyNameToTypeParameterMap().get(propertyName);
            if (typeParameterMap.hasTypeParameters()) {
                PropertyModel<?> concretePropertyModel = getSpecializedPropertyModel(model, propertyTypeParameters, typeParameterMap);
                concretePropertyModels.set(i, concretePropertyModel);
                if (concreteIdProperty != null && concreteIdProperty.getName().equals(propertyName)) {
                    concreteIdProperty = concretePropertyModel;
                }
            }
        }

        boolean discriminatorEnabled = changeTheDiscriminator ? propertyModel.useDiscriminator() : clazzModel.useDiscriminator();
        return new ClassModel<>(clazzModel.getType(), clazzModel.getPropertyNameToTypeParameterMap(),
                clazzModel.getInstanceCreatorFactory(), discriminatorEnabled, clazzModel.getDiscriminatorKey(),
                clazzModel.getDiscriminator(), IdPropertyModelHolder.create(clazzModel, concreteIdProperty), concretePropertyModels);
    }

    private <V> PropertyModel<V> getSpecializedPropertyModel(final PropertyModel<V> propertyModel,
                                                             final List<TypeData<?>> propertyTypeParameters,
                                                             final TypeParameterMap typeParameterMap) {
        TypeData<V> specializedPropertyType = specializeTypeData(propertyModel.getTypeData(), propertyTypeParameters, typeParameterMap);
        if (propertyModel.getTypeData().equals(specializedPropertyType)) {
            return propertyModel;
        }

        return new PropertyModel<>(propertyModel.getName(), propertyModel.getReadName(), propertyModel.getWriteName(),
                specializedPropertyType, null, propertyModel.getPropertySerialization(), propertyModel.useDiscriminator(),
                propertyModel.getPropertyAccessor(), propertyModel.getError(), propertyModel.getBsonRepresentation());
    }

}
