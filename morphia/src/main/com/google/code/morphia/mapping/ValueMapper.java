/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.mapping;

import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.DBObject;

import java.util.Map;

/**
 * Simple mapper that just uses the Mapper.getOptions().converts
 *
 * @author Scott Hernnadez
 */
class ValueMapper implements CustomMapper {
    public void toDBObject(final Object entity, final MappedField mf, final DBObject dbObject, final Map<Object,
                                                                                                        DBObject>
                                                                                               involvedObjects,
                           final Mapper mapr) {
        try {
            mapr.converters.toDBObject(entity, mf, dbObject, mapr.getOptions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void fromDBObject(final DBObject dbObject, final MappedField mf, final Object entity,
                             final EntityCache cache, final Mapper mapr) {
        try {
            mapr.converters.fromDBObject(dbObject, mf, entity);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
