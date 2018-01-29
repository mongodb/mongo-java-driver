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

package com.mongodb.util;

/**
 * <p>Exception that is thrown when invalid JSON is encountered by the parser.</p>
 *
 * <p>The error message is formatted so that it points to the first.</p>
 *
 * <p>This exception creates a message that points to the first offending character in the JSON string:</p>
 * <pre>
 * { "x" : 3, "y" : 4, some invalid json.... }
 *                     ^
 * </pre>
 */
public class JSONParseException extends RuntimeException {

    private static final long serialVersionUID = -4415279469780082174L;

    final String jsonString;
    final int pos;

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append(jsonString);
        sb.append("\n");
        for (int i = 0; i < pos; i++) {
            sb.append(" ");
        }
        sb.append("^");
        return sb.toString();
    }

    /**
     * Creates a new instance.
     *
     * @param jsonString the JSON being parsed
     * @param position   the position of the failure
     */
    public JSONParseException(final String jsonString, final int position) {
        this.jsonString = jsonString;
        this.pos = position;
    }

    /**
     * Creates a new instance.
     *
     * @param jsonString the JSON being parsed
     * @param position   the position of the failure
     * @param cause      the root cause
     */
    public JSONParseException(final String jsonString, final int position, final Throwable cause) {
        super(cause);
        this.jsonString = jsonString;
        this.pos = position;
    }
}
