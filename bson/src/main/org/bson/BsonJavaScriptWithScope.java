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

package org.bson;

/**
 * A representation of the JavaScript Code with Scope BSON type.
 *
 * @since 3.0
 */
public class BsonJavaScriptWithScope extends BsonValue {

    private final String code;
    private final BsonDocument scope;

    /**
     * Construct a new instance with the given code and scope.
     *
     * @param code the code
     * @param scope the scope
     */
    public BsonJavaScriptWithScope(final String code, final BsonDocument scope) {
        if (code == null) {
            throw new IllegalArgumentException("code can not be null");
        }
        if (scope == null) {
            throw new IllegalArgumentException("scope can not be null");
        }
        this.code = code;
        this.scope = scope;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.JAVASCRIPT_WITH_SCOPE;
    }

    /**
     * Get the code.
     *
     * @return the code
     */
    public String getCode() {
        return code;
    }

    /**
     * Get the scope.
     *
     * @return the scope
     */
    public BsonDocument getScope() {
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

        BsonJavaScriptWithScope that = (BsonJavaScriptWithScope) o;

        if (!code.equals(that.code)) {
            return false;
        }
        if (!scope.equals(that.scope)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = code.hashCode();
        result = 31 * result + scope.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BsonJavaScriptWithScope{"
               + "code=" + getCode()
               + "scope=" + scope
               + '}';
    }

    static BsonJavaScriptWithScope clone(final BsonJavaScriptWithScope from) {
        return new BsonJavaScriptWithScope(from.code, from.scope.clone());
    }
}

