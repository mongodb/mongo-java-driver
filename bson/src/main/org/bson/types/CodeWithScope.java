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

package org.bson.types;

import org.bson.Document;

/**
 * A representation of the JavaScript Code with Scope BSON type.
 *
 * @since 3.0
 */
public class CodeWithScope extends Code {

    private final Document scope;

    private static final long serialVersionUID = -6284832275113680002L;

    /**
     * Construct an instance.
     *
     * @param code the code
     * @param scope the scope
     */
    public CodeWithScope(final String code, final Document scope) {
        super(code);
        this.scope = scope;
    }

    /**
     * Gets the scope, which is is a mapping from identifiers to values, representing the scope in which the code should be evaluated.
     *
     * @return the scope
     */
    public Document getScope() {
        return scope;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        CodeWithScope that = (CodeWithScope) o;

        if (scope != null ? !scope.equals(that.scope) : that.scope != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getCode().hashCode() ^ scope.hashCode();
    }
}

