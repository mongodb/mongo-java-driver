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

/**
 * @author Scott Hernandez
 */
public enum UpdateOperator {
    SET("$set"),
    UNSET("$unset"),
    PULL("$pull"),
    PULL_ALL("$pullAll"),
    PUSH("$push"),
    PUSH_ALL("$pushAll"),
    ADD_TO_SET("$addToSet"),
    ADD_TO_SET_EACH("$addToSet"),
    // fake to indicate that the value should be wrapped in an $each
    EACH("$each"),
    POP("$pop"),
    INC("$inc"),
    Foo("$foo");

    private final String value;

    private UpdateOperator(final String val) {
        value = val;
    }

    private boolean equals(final String val) {
        return value.equals(val);
    }

    public String val() {
        return value;
    }

    public static UpdateOperator fromString(final String val) {
        for (int i = 0; i < values().length; i++) {
            final UpdateOperator fo = values()[i];
            if (fo.equals(val)) {
                return fo;
            }
        }
        return null;
    }
}