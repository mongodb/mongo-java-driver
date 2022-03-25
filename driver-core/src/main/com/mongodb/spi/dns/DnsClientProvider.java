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
 * A provider interface for {@link DnsClient}.
 *
 * <p>
 * The driver discovers implementations of this interface via {@link java.util.ServiceLoader}.
 * </p>
 *
 * @since 4.6
 * @see java.util.ServiceLoader
 */
public interface DnsClientProvider {
    /**
     * Construct a new instance of a {@link DnsClient}.
     *
     * @return a {@link DnsClient}
     */
    DnsClient create();
}
