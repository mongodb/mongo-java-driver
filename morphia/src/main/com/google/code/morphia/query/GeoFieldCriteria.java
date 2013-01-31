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

package com.google.code.morphia.query;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import java.util.Map;

public class GeoFieldCriteria extends FieldCriteria {

    Map<String, Object> opts = null;

    protected GeoFieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator op,
                               final Object value, final boolean validateNames, final boolean validateTypes,
                               final Map<String, Object> opts) {
        super(query, field, op, value, validateNames, validateTypes);
        this.opts = opts;
    }

    @Override
    public void addTo(final DBObject obj) {
        BasicDBObjectBuilder query = null;
        switch (operator) {
            case NEAR:
                query = BasicDBObjectBuilder.start(FilterOperator.NEAR.val(), value);
                break;
            case NEAR_SPHERE:
                query = BasicDBObjectBuilder.start(FilterOperator.NEAR_SPHERE.val(), value);
                break;
            case WITHIN_BOX:
                query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
                break;
            case WITHIN_CIRCLE:
                query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
                break;
            case WITHIN_CIRCLE_SPHERE:
                query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
                break;
            default:
                throw new UnsupportedOperationException(operator + " not supported for geo-query");
        }

        //add options...
        if (opts != null) {
            for (final Map.Entry<String, Object> e : opts.entrySet()) {
                query.append(e.getKey(), e.getValue());
            }
        }

        obj.put(field, query.get());
    }
}
