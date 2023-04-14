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
 * Service-provider class for {@link DnsClient}.
 *
 * <p> A resolver provider is a factory for custom implementations of
 * {@linkplain DnsClient a DNS client}. A DNS client defines operations for
 * looking up DNS records for a given type.
 *
 * <p>The driver discovers implementations of this interface via {@link java.util.ServiceLoader}.
 *
 * <p>If more fine-grained control is required for multi-tenant applications, an
 * {@linkplain DnsClient a DNS client} can be configured via
 * {@link com.mongodb.MongoClientSettings.Builder#dnsClient(DnsClient)}.
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
