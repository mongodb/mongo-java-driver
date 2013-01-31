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

import com.mongodb.DBObject;
import org.bson.types.CodeWScope;

public class WhereCriteria extends AbstractCriteria implements Criteria {

    private final Object js;

    public WhereCriteria(final String js) {
        this.js = js;
    }

    public WhereCriteria(final CodeWScope js) {
        this.js = js;
    }

    public void addTo(final DBObject obj) {
        obj.put(FilterOperator.WHERE.val(), this.js);
    }

    public String getFieldName() {
        return FilterOperator.WHERE.val();
    }

}
