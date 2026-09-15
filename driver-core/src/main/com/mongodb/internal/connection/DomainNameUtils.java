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

import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Pattern;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DomainNameUtils {
    private static final Pattern DOMAIN_PATTERN =
            Pattern.compile("^(?=.{1,255}$)((([a-zA-Z0-9]([a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])?\\.)+[a-zA-Z]{2,63}|localhost))$");

    static boolean isDomainName(final String domainName) {
        return DOMAIN_PATTERN.matcher(domainName).matches();
    }

    /**
     * Validates and normalizes a {@code srvAllowedHostsSuffix} value for use as the domain in SRV host validation.
     * A leading {@code "."} is prepended if absent, so the returned value always begins with {@code "."}; this is what
     * is stored and returned to callers. The suffix must contain at least one non-empty domain label and no
     * whitespace; an overly broad suffix is permitted and is the caller's responsibility.
     *
     * @param srvAllowedHostsSuffix the non-null suffix to validate
     * @return the normalized suffix, always beginning with {@code "."}
     * @throws IllegalArgumentException if the suffix contains whitespace, contains no domain label (it is empty or
     * consists only of a leading {@code "."}), or contains an empty domain label (consecutive {@code "."} characters
     * or a trailing {@code "."})
     */
    public static String normalizeSrvAllowedHostsSuffix(final String srvAllowedHostsSuffix) {
        if (containsWhitespace(srvAllowedHostsSuffix)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must not contain whitespace");
        }
        // A single leading '.' is allowed (it is the documented suffix form); everything after it must be one or more
        // non-empty domain labels.
        boolean hasLeadingDot = srvAllowedHostsSuffix.startsWith(".");
        String labels = hasLeadingDot ? srvAllowedHostsSuffix.substring(1) : srvAllowedHostsSuffix;
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must contain at least one domain label");
        }
        String[] parts = labels.split("\\.", -1);
        for (String label : parts) {
            if (label.isEmpty()) {
                throw new IllegalArgumentException("srvAllowedHostsSuffix must not contain empty domain labels");
            }
        }
        if (isTopLevelDomain(hasLeadingDot ? srvAllowedHostsSuffix.substring(1) : srvAllowedHostsSuffix)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must not be a top-level domain");
        }
        return hasLeadingDot ? srvAllowedHostsSuffix : "." + srvAllowedHostsSuffix;
    }

    private static boolean containsWhitespace(final String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isTopLevelDomain(String suffix) {
        try (Scanner scanner = new Scanner(Objects.requireNonNull(
                DomainNameUtils.class.getResourceAsStream("public_suffix_list.dat"), "Missing DNS suffix list"))) {
            String line;
            int firstDot = suffix.indexOf('.');
            String rootDomain = firstDot >= 0 ? suffix.substring(firstDot + 1) : suffix;
            boolean invalidMatchWildcard = false;
            while ((line = scanner.nextLine()) != null) {
                if (line.startsWith("//") || line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("!")) {
                    if (invalidMatchWildcard && suffix.equals(line.substring(1))) {
                        return false;
                    }
                } else if (line.startsWith("*")) {
                    String lineSuffix = line.substring(2);
                    if (suffix.equals(lineSuffix) || rootDomain.equals(lineSuffix)) {
                        invalidMatchWildcard = true;
                    }
                } else {
                    if (invalidMatchWildcard) {
                        return true;
                    }
                    if (suffix.equals(line)) {
                        return true;
                    }
                }
            }
            return invalidMatchWildcard;
        }
    }
}
