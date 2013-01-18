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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("CanBeFinal")
public class CriteriaContainerImpl extends AbstractCriteria implements Criteria, CriteriaContainer {
    protected CriteriaJoin joinMethod;
    protected List<Criteria> children;

    protected QueryImpl<?> query;

    protected CriteriaContainerImpl(final CriteriaJoin joinMethod) {
        this.joinMethod = joinMethod;
        this.children = new ArrayList<Criteria>();
    }

    protected CriteriaContainerImpl(final QueryImpl<?> query, final CriteriaJoin joinMethod) {
        this(joinMethod);
        this.query = query;
    }

    public void add(final Criteria... criteria) {
        for (final Criteria c : criteria) {
            c.attach(this);
            this.children.add(c);
        }
    }

    public void remove(final Criteria criteria) {
        this.children.remove(criteria);
    }

    public void addTo(final DBObject obj) {
        if (this.joinMethod == CriteriaJoin.AND) {
            final Set<String> fields = new HashSet<String>();
            int nonNullFieldNames = 0;
            for (final Criteria child : this.children) {
                if (null != child.getFieldName()) {
                    fields.add(child.getFieldName());
                    nonNullFieldNames++;
                }
            }
            if (fields.size() < nonNullFieldNames) {
                //use $and
                final BasicDBList and = new BasicDBList();

                for (final Criteria child : this.children) {
                    final BasicDBObject container = new BasicDBObject();
                    child.addTo(container);
                    and.add(container);
                }

                obj.put("$and", and);
            }
            else {
                //no dup field names, don't use $and
                for (final Criteria child : this.children) {
                    child.addTo(obj);
                }
            }
        }
        else if (this.joinMethod == CriteriaJoin.OR) {
            final BasicDBList or = new BasicDBList();

            for (final Criteria child : this.children) {
                final BasicDBObject container = new BasicDBObject();
                child.addTo(container);
                or.add(container);
            }

            obj.put("$or", or);
        }
    }

    public CriteriaContainer and(final Criteria... criteria) {
        return collect(CriteriaJoin.AND, criteria);
    }

    public CriteriaContainer or(final Criteria... criteria) {
        return collect(CriteriaJoin.OR, criteria);
    }

    private CriteriaContainer collect(final CriteriaJoin cj, final Criteria... criteria) {
        final CriteriaContainerImpl parent = new CriteriaContainerImpl(this.query, cj);

        for (final Criteria c : criteria) {
            parent.add(c);
        }

        add(parent);

        return parent;
    }

    public FieldEnd<? extends CriteriaContainer> criteria(final String name) {
        return this.criteria(name, this.query.isValidatingNames());
    }

    private FieldEnd<? extends CriteriaContainer> criteria(final String field, final boolean validateName) {
        return new FieldEndImpl<CriteriaContainerImpl>(this.query, field, this, validateName);
    }

    public String getFieldName() {
        return joinMethod.toString();
    }
}
