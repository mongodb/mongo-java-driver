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

import java.net.IDN;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class DomainNameUtils {
    private static final Pattern DOMAIN_PATTERN = Pattern.compile(
            "^(?=.{1,255}$)(([a-z0-9]([a-z0-9\\-]{0,61}[a-z0-9])?\\.)*[a-z0-9]([a-z0-9\\-]{0,61}[a-z0-9])?)$", Pattern.CASE_INSENSITIVE);
    private static final Set<String> VALID_SINGLE_LABEL_SUFFIXES = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(
            // RFC 6761 special use names
            "test", "localhost", "invalid", "example",
            // RFC 6762 multicast DNS
            "local",
            // reserved by ICANN for private use
            "internal",
            // not reserved by ICANN, but commonly used privately
            "corp", "home", "mail")));

    static boolean isDomainName(final String domainName) {
        return DOMAIN_PATTERN.matcher(domainName).matches();
    }

    /**
     * Validates and normalizes a {@code srvAllowedHostsSuffix} value for use as the domain in SRV host validation.
     * A leading {@code "."} is prepended if absent, so the returned value always begins with {@code "."}; this is what
     * is stored and returned to callers. The suffix must contain at least one non-empty domain label and no
     * whitespace. An overly broad suffix is restricted based on a public suffix list.
     *
     * @param srvAllowedHostsSuffix the non-null suffix to validate
     * @return the normalized suffix, always beginning with {@code "."}
     * @throws IllegalArgumentException if the suffix contains whitespace, contains no domain label (it is empty or
     * consists only of a leading {@code "."}), contains an empty domain label (consecutive {@code "."} characters
     * or a trailing {@code "."}), or, is a public domain suffix (e.g., top-level domain, AWS data center)
     */
    public static String normalizeSrvAllowedHostsSuffix(final String srvAllowedHostsSuffix) {
        if (containsWhitespace(srvAllowedHostsSuffix)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must not contain whitespace");
        }
        // A single leading '.' is allowed (it is the documented suffix form); everything after it must be one or more
        // non-empty domain labels.
        boolean hasLeadingDot = srvAllowedHostsSuffix.startsWith(".");
        String labels = hasLeadingDot ? srvAllowedHostsSuffix.substring(1) : srvAllowedHostsSuffix;
        if (labels.endsWith(".")) {
            labels = labels.substring(0, labels.length() - 1);
        }
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must contain at least one domain label");
        }
        try {
            labels = IDN.toASCII(labels, IDN.ALLOW_UNASSIGNED).toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix is not a valid domain: " + srvAllowedHostsSuffix, e);
        }
        if (!isDomainName(labels)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix is not a valid domain: " + srvAllowedHostsSuffix);
        }
        if (isPublicSuffix(labels)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must not be a public domain suffix");
        }
        if (!labels.contains(".") && !VALID_SINGLE_LABEL_SUFFIXES.contains(labels)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must contain two or more domain labels");
        }
        return "." + labels;
    }

    private static boolean containsWhitespace(final String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isPublicSuffix(final String suffix) {
        try (Scanner scanner = new Scanner(
                Objects.requireNonNull(
                        DomainNameUtils.class.getResourceAsStream("public_suffix_list.dat"),
                        "Missing DNS public suffix list"),
                "UTF-8")) {
            int firstDot = suffix.indexOf('.');
            String rootDomain = firstDot >= 0 ? suffix.substring(firstDot + 1) : suffix;
            boolean invalidMatchWildcard = false;
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.startsWith("!")) {
                    if (invalidMatchWildcard && suffix.equals(IDN.toASCII(line.substring(1), IDN.ALLOW_UNASSIGNED))) {
                        return false;
                    }
                } else if (line.startsWith("*")) {
                    String lineSuffix = IDN.toASCII(line.substring(2), IDN.ALLOW_UNASSIGNED);
                    if (suffix.equals(lineSuffix) || rootDomain.equals(lineSuffix)) {
                        invalidMatchWildcard = true;
                    }
                } else {
                    if (invalidMatchWildcard) {
                        return true;
                    }
                    if (suffix.equals(IDN.toASCII(line, IDN.ALLOW_UNASSIGNED))) {
                        return true;
                    }
                }
            }
            return invalidMatchWildcard;
        }
    }
}
