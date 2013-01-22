/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.DBObject;

import java.util.Map;

/**
 * A CustomMapper if one that implements the methods needed to map to/from POJO/DBObject
 *
 * @author skot
 */
public interface CustomMapper {
    void toDBObject(Object entity, MappedField mf, DBObject dbObject, Map<Object, DBObject> involvedObjects,
                    Mapper mapr);

    void fromDBObject(DBObject dbObject, MappedField mf, Object entity, EntityCache cache, Mapper mapr);
}
