/*
 * Copyright 2008-2017 MongoDB, Inc.
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

package org.bson.codecs.configuration;

/**
 * An exception indicating that a codec registry has been misconfigured in some way, preventing it from providing a codec for the
 * requested class.
 *
 * @since 3.0
 */
public class CodecConfigurationException extends RuntimeException {

    private static final long serialVersionUID = -5656763889202800056L;

    /**
     * Construct a new instance.
     *
     * @param msg the message
     */
    public CodecConfigurationException(final String msg) {
        super(msg);
    }

    /**
     * Construct a new instance and wraps a cause
     *
     * @param message the message
     * @param cause   the underlying cause
     * @since 3.5
     */
    public CodecConfigurationException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
