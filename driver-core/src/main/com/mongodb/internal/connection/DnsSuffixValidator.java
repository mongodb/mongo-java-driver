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

import com.mongodb.connection.SrvHostValidator;

import java.net.IDN;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.DomainNameUtils.isDomainName;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class DnsSuffixValidator implements SrvHostValidator {
    private static final Set<String> VALID_SINGLE_LABEL_SUFFIXES = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(
            // RFC 6761 special use names
            "test", "localhost", "invalid", "example",
            // RFC 6762 multicast DNS
            "local",
            // reserved by ICANN for private use
            "internal",
            // not reserved by ICANN, but commonly used privately
            "corp", "home", "mail")));

    private static boolean containsWhitespace(final String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private final String suffix;
    private final boolean isAllowedSingleLabel;

    /**
     * Creates a validator from a suffix which be validated and normalized.
     *
     * <p>This is used by {@code srvAllowedHostsSuffix} for the domain in SRV host validation.
     *
     * <p>A leading {@code "."} is prepended if absent, so the returned value always begins with {@code "."}; this is what
     * is stored and returned to callers. The suffix must contain at least one non-empty domain label and no
     * whitespace.</p>
     *
     * @param suffix the non-null suffix to use
     * @throws IllegalArgumentException if the suffix contains whitespace, contains no domain label (it is empty or
     * consists only of a leading {@code "."}), contains an empty domain label (consecutive {@code "."} characters
     * or a trailing {@code "."}), or, top-level domain
     */
    public DnsSuffixValidator(final String suffix) {
        notNull("srvAllowedHostsSuffix", suffix);
        if (containsWhitespace(suffix)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must not contain whitespace");
        }
        // A single leading '.' is allowed (it is the documented suffix form); everything after it must be one or more
        // non-empty domain labels.
        boolean hasLeadingDot = suffix.startsWith(".");
        String labels = hasLeadingDot ? suffix.substring(1) : suffix;
        if (labels.endsWith(".")) {
            labels = labels.substring(0, labels.length() - 1);
        }
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must contain at least one domain label");
        }
        try {
            labels = IDN.toASCII(labels, IDN.ALLOW_UNASSIGNED).toLowerCase(Locale.ROOT);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix is not a valid domain: " + suffix, e);
        }
        if (!isDomainName(labels)) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix is not a valid domain: " + suffix);
        }
        isAllowedSingleLabel = VALID_SINGLE_LABEL_SUFFIXES.contains(labels);
        if (!isAllowedSingleLabel && !labels.contains(".")) {
            throw new IllegalArgumentException("srvAllowedHostsSuffix must contain at least two domain labels");
        }
        this.suffix = "." + labels;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DnsSuffixValidator that = (DnsSuffixValidator) o;
        return Objects.equals(suffix, that.suffix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(suffix);
    }

    /**
     * The normalized suffix that is used for validation
     * @return the domain name suffix
     */
    public String getSuffix() {
        return suffix;
    }

    /**
     * Whether the label is one of the special single-label domains permitted
     * @return true if the domain can be a single label
     */
    public boolean isAllowedSingleLabel() {
        return isAllowedSingleLabel;
    }

    @Override
    public boolean isValidHost(final String discoveredHostName) {
        return discoveredHostName.endsWith(suffix);
    }

    @Override
    public String toString() {
        return suffix;
    }
}
