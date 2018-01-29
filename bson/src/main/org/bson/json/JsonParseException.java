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

package org.bson.json;


import static java.lang.String.format;

/**
 * JSONParseException indicates some exception happened during JSON processing.
 *
 * @since 3.0
 */
public class JsonParseException extends RuntimeException {


    private static final long serialVersionUID = -6722022620020198727L;

    /**
     * Constructs a new runtime exception with null as its detail message.
     */
    public JsonParseException() {
        super();
    }

    /**
     * Constructs a new runtime exception with the specified detail message.
     *
     * @param s The detail message.
     */
    public JsonParseException(final String s) {
        super(s);
    }

    /**
     * Constructs a new runtime exception with string formatted using specified pattern and arguments.
     *
     * @param pattern A {@link  java.util.Formatter format string}.
     * @param args    the arguments to insert into the pattern String
     */
    public JsonParseException(final String pattern, final Object... args) {
        super(format(pattern, args));
    }

    /**
     * Create a JSONParseException with the given {@link Throwable} cause.
     *
     * @param t the throwable root case
     */
    public JsonParseException(final Throwable t) {
        super(t);
    }
}
