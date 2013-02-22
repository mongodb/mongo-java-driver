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

// CodeWScope.java

package org.bson.types;

import com.mongodb.DBObject;

/**
 * for using the CodeWScope type
 */
public class CodeWScope extends Code {

    private final DBObject scope;

    private static final long serialVersionUID = -6284832275113680002L;

    public CodeWScope(final String code, final DBObject scope) {
        super(code);
        this.scope = scope;
    }

    public DBObject getScope() {
        return scope;
    }

    public boolean equals(final Object o) {
        if (!(o instanceof CodeWScope)) {
            return false;
        }

        final CodeWScope c = (CodeWScope) o;
        return getCode().equals(c.getCode()) && scope.equals(c.scope);
    }

    public int hashCode() {
        return getCode().hashCode() ^ scope.hashCode();
    }
}

