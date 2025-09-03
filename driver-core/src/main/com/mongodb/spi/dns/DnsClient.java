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

import com.mongodb.MongoClientSettings;
import com.mongodb.annotations.ThreadSafe;

import java.util.List;


/**
 * An interface describing a DNS client.
 *
 * @since 4.6
 * @see DnsClientProvider
 * @see MongoClientSettings.Builder#dnsClient(DnsClient)
 */
@ThreadSafe
public interface DnsClient {
    /**
     * Gets the resource record values for the given name and type.
     *
     * <p>
     * Implementations should throw {@link DnsWithResponseCodeException} if the DNS response code is known.  Otherwise, the more generic
     * {@link DnsException} should be thrown.
     * </p>
     *
     * @param name the name of the resource to look up
     * @param type the resource record type, typically either {@code "SRV"} or {@code "TXT"}.
     * @return the list of values for the requested resource, or the empty list if none exist
     * @throws DnsException the exception
     */
    List<String> getResourceRecordData(String name, String type) throws DnsException;
}
