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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * This interface defines operations for looking up host names.
 *
 * <p> The default resolver for the driver can be customized by deploying an implementation of {@link InetAddressResolverProvider}.</p>
 *
 * @see InetAddressResolverProvider
 * @see MongoClientSettings.Builder#inetAddressResolver(InetAddressResolver)
 * @since 4.10
 */
@ThreadSafe
public interface InetAddressResolver {
    /**
     * Given the name of a host, returns a list of IP addresses of the requested
     * address family associated with a provided hostname.
     *
     * <p>Implementations are encouraged to implement their own caching policies, as there is
     * no guarantee that the caller will implement a cache.
     *
     * @param host the host
     * @return a list of IP addresses for the requested host
     * @throws UnknownHostException if no IP addresses for the {@code host} could be found
     */
    List<InetAddress> lookupByName(String host) throws UnknownHostException;
}
