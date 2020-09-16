/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.BsonObjectId;
import org.bson.BsonType;
import org.bson.types.ObjectId;

final class ConventionObjectIdGeneratorsImpl implements Convention {
    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
       if (classModelBuilder.getIdGenerator() == null && classModelBuilder.getIdPropertyName() != null) {
           PropertyModelBuilder<?> idProperty = classModelBuilder.getProperty(classModelBuilder.getIdPropertyName());
           if (idProperty != null) {
               Class<?> idType = idProperty.getTypeData().getType();
               if (classModelBuilder.getIdGenerator() == null && idType.equals(ObjectId.class)) {
                   classModelBuilder.idGenerator(IdGenerators.OBJECT_ID_GENERATOR);
               } else if (classModelBuilder.getIdGenerator() == null && idType.equals(BsonObjectId.class)) {
                   classModelBuilder.idGenerator(IdGenerators.BSON_OBJECT_ID_GENERATOR);
               } else if (classModelBuilder.getIdGenerator() == null && idType.equals(String.class)
                       && idProperty.getBsonRepresentation() == BsonType.OBJECT_ID) {
                   classModelBuilder.idGenerator(IdGenerators.STRING_ID_GENERATOR);
               }
           }
       }
    }
}
