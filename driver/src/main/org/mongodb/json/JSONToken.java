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

package org.mongodb.json;

import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;

/**
 * A JSON token.
 */
public class JSONToken {

    private final String lexeme;
    private final JSONTokenType type;

    public JSONToken(final JSONTokenType type, final String lexeme) {
        this.lexeme = lexeme;
        this.type = type;
    }

    public String getLexeme() {
        return lexeme;
    }

    public JSONTokenType getType() {
        return type;
    }

    public String asString() {
        throw new UnsupportedOperationException();
    }

    public ObjectId asObjectId() {
        throw new UnsupportedOperationException();
    }

    public long asDateTime() {
        throw new UnsupportedOperationException();
    }

    public double asDouble() {
        throw new UnsupportedOperationException();
    }

    public int asInt32() {
        throw new UnsupportedOperationException();
    }

    public long asInt64() {
        throw new UnsupportedOperationException();
    }

    public RegularExpression asRegularExpression() {
        throw new UnsupportedOperationException();
    }

}
