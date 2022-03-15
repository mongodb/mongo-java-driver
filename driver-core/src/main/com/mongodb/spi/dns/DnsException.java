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

package com.mongodb.spi.dns;

/**
 * An exception indicating a DNS error;
 *
 * @since 4.6
 * @see DnsClient#getAttributeValues(String, String)
 */
public class DnsException extends Exception {
    private static final long serialVersionUID = 1;

    /**
     * Construct an instance
     *
     * @param message the message
     * @param cause the cause
     */
    public DnsException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
