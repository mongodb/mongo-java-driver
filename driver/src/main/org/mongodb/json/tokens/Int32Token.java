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

package org.mongodb.json.tokens;

import org.mongodb.json.JSONToken;
import org.mongodb.json.JSONTokenType;

public class Int32Token extends JSONToken {

    private final int value;

    public Int32Token(final String lexeme, final int value) {
        super(JSONTokenType.INT32, lexeme);
        this.value = value;
    }

    @Override
    public int asInt32() {
        return value;
    }

    @Override
    public long asInt64() {
        return value;
    }
}
