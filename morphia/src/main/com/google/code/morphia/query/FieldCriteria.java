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

package com.google.code.morphia.query;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FieldCriteria extends AbstractCriteria implements Criteria {
    private static final Logr log = MorphiaLoggerFactory.get(FieldCriteria.class);

    protected final String field;
    protected final FilterOperator operator;
    protected final Object value;
    protected final boolean not;

    @SuppressWarnings("unchecked")
    protected FieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator op,
                            final Object value, final boolean validateNames, final boolean validateTypes) {
        this(query, field, op, value, validateNames, validateTypes, false);
    }

    protected FieldCriteria(final QueryImpl<?> query, String field, final FilterOperator op, final Object value,
                            final boolean validateNames, final boolean validateTypes, final boolean not) {
        final StringBuffer sb = new StringBuffer(field); //validate might modify prop string to translate java field
        // name to db field name
        final MappedField mf = Mapper.validate(query.getEntityClass(), query.getDatastore().getMapper(), sb, op,
                                              value, validateNames, validateTypes);
        field = sb.toString();

        final Mapper mapr = query.getDatastore().getMapper();

        MappedClass mc = null;
        try {
            if (value != null && !ReflectionUtils.isPropertyType(value.getClass()) && !ReflectionUtils
                                                                                       .implementsInterface(value
                                                                                                            .getClass(),
                                                                                                           Iterable
                                                                                                           .class)) {
                if (mf != null && !mf.isTypeMongoCompatible()) {
                    mc = mapr.getMappedClass((mf.isSingleValue()) ? mf.getType() : mf.getSubClass());
                }
                else {
                    mc = mapr.getMappedClass(value);
                }
            }
        } catch (Exception e) {
            //Ignore these. It is likely they related to mapping validation that is unimportant for queries (the
            // query will fail/return-empty anyway)
            log.debug("Error during mapping of filter criteria: ", e);
        }

        Object mappedValue = mapr.toMongoObject(mf, mc, value);

        final Class<?> type = (mappedValue == null) ? null : mappedValue.getClass();

        //convert single values into lists for $in/$nin
        if (type != null && (op == FilterOperator.IN || op == FilterOperator.NOT_IN) && !type.isArray() && !Iterable
                                                                                                            .class








                                                                                                            .isAssignableFrom(

                                                                                                                             type)) {
            mappedValue = Collections.singletonList(mappedValue);
        }

        //TODO: investigate and/or add option to control this.
        if (op == FilterOperator.ELEMENT_MATCH && mappedValue instanceof DBObject) {
            ((DBObject) mappedValue).removeField(Mapper.ID_KEY);
        }

        this.field = field;
        this.operator = op;
        if (not) {
            this.value = new BasicDBObject("$not", mappedValue);
        }
        else {
            this.value = mappedValue;
        }
        this.not = not;
    }

    @SuppressWarnings("unchecked")
    public void addTo(final DBObject obj) {
        if (FilterOperator.EQUAL.equals(operator)) {
            obj.put(this.field, value); // no operator, prop equals value

        }
        else {
            Object inner = obj.get(field); // operator within inner object

            if (!(inner instanceof Map)) {
                inner = new HashMap<String, Object>();
                obj.put(field, inner);
            }
            final Object val = not ? new BasicDBObject("$not", value) : value;
            ((Map<String, Object>) inner).put(operator.val(), val);
        }
    }

    public String getFieldName() {
        return field;
    }

    @Override
    public String toString() {
        return this.field + " " + this.operator.val() + " " + this.value;
    }
}
