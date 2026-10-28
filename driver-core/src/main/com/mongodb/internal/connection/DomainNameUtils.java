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
package com.mongodb.internal.connection;

import java.util.regex.Pattern;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DomainNameUtils {
    private static final Pattern DOMAIN_PATTERN = Pattern.compile(
            "^(?=.{1,255}$)(([a-z0-9_]([a-z0-9\\-_]{0,61}[a-z0-9_])?\\.)*(?![0-9]+$)[a-z0-9_]([a-z0-9\\-_]{0,61}[a-z0-9_])?)$",
            Pattern.CASE_INSENSITIVE);

    static boolean isDomainName(final String domainName) {
        return DOMAIN_PATTERN.matcher(domainName).matches();
    }

    /**
     * Deliberately looser than {@link #isDomainName(String)} (RFC 1123): it applies no length limits and no hyphen-position
     * rules, and allows underscore, so that host names that resolve and are in use today are not rejected. It only
     * guarantees that the name cannot be re-parsed as anything but a single host (no brackets, commas, colons, slashes, ...).
     */
    private static final Pattern SRV_HOST_NAME_PATTERN = Pattern.compile("[a-z0-9_-]+(\\.[a-z0-9_-]+)*");

    /**
     * @param hostName a host name already normalized (A-label form, lower case, single trailing dot stripped)
     */
    public static boolean isValidSrvHostName(final String hostName) {
        return SRV_HOST_NAME_PATTERN.matcher(hostName).matches();
    }
}
