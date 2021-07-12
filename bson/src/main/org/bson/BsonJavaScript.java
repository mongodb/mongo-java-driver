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
 * For using the JavaScript Code type.
 *
 * @since 3.0
 */
public class BsonJavaScript extends BsonValue {

    private final String code;

    /**
     * Construct a new instance with the given JavaScript code.
     *
     * @param code the JavaScript code
     */
    public BsonJavaScript(final String code) {
        this.code = code;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.JAVASCRIPT;
    }

    /**
     * Get the JavaScript code.
     *
     * @return the code
     */
    public String getCode() {
        return code;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonJavaScript code1 = (BsonJavaScript) o;

        if (!code.equals(code1.code)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return code.hashCode();
    }

    @Override
    public String toString() {
        return "BsonJavaScript{"
               + "code='" + code + '\''
               + '}';
    }
}

