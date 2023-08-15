/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright (C) 2008 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.mongodb.internal.connection;

import com.mongodb.lang.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * Static utility methods pertaining to {@link InetAddress} instances.
 *
 * <p><b>Important note:</b> Unlike {@link  java.net.InetAddress#getByName(String)}, the methods of this class never
 * cause DNS services to be accessed. For this reason, you should prefer these methods as much as
 * possible over their JDK equivalents whenever you are expecting to handle only IP address string
 * literals -- there is no blocking DNS penalty for a malformed string.
 */
final class InetAddresses {
    private static final int IPV4_PART_COUNT = 4;
    private static final int IPV6_PART_COUNT = 8;
    private static final char IPV4_DELIMITER = '.';
    private static final char IPV6_DELIMITER = ':';

    private InetAddresses() {
    }

    /**
     * Returns the {@link InetAddress} having the given string representation.
     *
     * <p>This deliberately avoids all nameservice lookups (e.g. no DNS).
     *
     * <p>Anything after a {@code %} in an IPv6 address is ignored (assumed to be a Scope ID).
     *
     * <p>This method accepts non-ASCII digits, for example {@code "１９２.１６８.０.１"} (those are fullwidth
     * characters). That is consistent with {@link InetAddress}, but not with various RFCs.
     *
     * @param ipString {@code String} containing an IPv4 or IPv6 string literal, e.g. {@code
     *                 "192.168.0.1"} or {@code "2001:db8::1"}
     * @return {@link InetAddress} representing the argument
     * @throws IllegalArgumentException if the argument is not a valid IP string literal
     */
    static InetAddress forString(String ipString) {
        byte[] addr = ipStringToBytes(ipString);

        // The argument was malformed, i.e. not an IP string literal.
        if (addr == null) {
            throw new IllegalArgumentException(ipString + " IP address is incorrect");
        }

        return bytesToInetAddress(addr);
    }

    /**
     * Returns {@code true} if the supplied string is a valid IP string literal, {@code false}
     * otherwise.
     *
     * <p>This method accepts non-ASCII digits, for example {@code "１９２.１６８.０.１"} (those are fullwidth
     * characters). That is consistent with {@link InetAddress}, but not with various RFCs.
     *
     * @param ipString {@code String} to evaluated as an IP string literal
     * @return {@code true} if the argument is a valid IP string literal
     */
    static boolean isInetAddress(String ipString) {
        return ipStringToBytes(ipString) != null;
    }

    /**
     * Returns {@code null} if unable to parse into a {@code byte[]}.
     */
    @Nullable
    static byte[] ipStringToBytes(String ipStringParam) {
        String ipString = ipStringParam;
        // Make a first pass to categorize the characters in this string.
        boolean hasColon = false;
        boolean hasDot = false;
        int percentIndex = -1;
        for (int i = 0; i < ipString.length(); i++) {
            char c = ipString.charAt(i);
            if (c == '.') {
                hasDot = true;
            } else if (c == ':') {
                if (hasDot) {
                    return null; // Colons must not appear after dots.
                }
                hasColon = true;
            } else if (c == '%') {
                percentIndex = i;
                break; // everything after a '%' is ignored (it's a Scope ID): http://superuser.com/a/99753
            } else if (Character.digit(c, 16) == -1) {
                return null; // Everything else must be a decimal or hex digit.
            }
        }

        // Now decide which address family to parse.
        if (hasColon) {
            if (hasDot) {
                ipString = convertDottedQuadToHex(ipString);
                if (ipString == null) {
                    return null;
                }
            }
            if (percentIndex != -1) {
                ipString = ipString.substring(0, percentIndex);
            }
            return textToNumericFormatV6(ipString);
        } else if (hasDot) {
            if (percentIndex != -1) {
                return null; // Scope IDs are not supported for IPV4
            }
            return textToNumericFormatV4(ipString);
        }
        return null;
    }

    private static boolean hasCorrectNumberOfOctets(final String sequence) {
        int matches = 3;
        int index = 0;
        while (matches-- > 0) {
            index = sequence.indexOf(IPV4_DELIMITER, index);
            if (index == -1) {
                return false;
            }
            index++;
        }
        return sequence.indexOf(IPV4_DELIMITER, index) == -1;
    }

    private static int countIn(final CharSequence sequence, final char character) {
        int count = 0;
        for (int i = 0; i < sequence.length(); i++) {
            if (sequence.charAt(i) == character) {
                count++;
            }
        }
        return count;
    }

    @Nullable
    private static byte[] textToNumericFormatV4(String ipString) {
        if (!hasCorrectNumberOfOctets(ipString)) {
            return null; // Wrong number of parts
        }

        byte[] bytes = new byte[IPV4_PART_COUNT];
        int start = 0;
        // Iterate through the parts of the ip string.
        // Invariant: start is always the beginning of an octet.
        for (int i = 0; i < IPV4_PART_COUNT; i++) {
            int end = ipString.indexOf(IPV4_DELIMITER, start);
            if (end == -1) {
                end = ipString.length();
            }
            try {
                bytes[i] = parseOctet(ipString, start, end);
            } catch (NumberFormatException ex) {
                return null;
            }
            start = end + 1;
        }

        return bytes;
    }

    @Nullable
    private static byte[] textToNumericFormatV6(String ipString) {
        // An address can have [2..8] colons.
        int delimiterCount = countIn(ipString, IPV6_DELIMITER);
        if (delimiterCount < 2 || delimiterCount > IPV6_PART_COUNT) {
            return null;
        }
        int partsSkipped = IPV6_PART_COUNT - (delimiterCount + 1); // estimate; may be modified later
        boolean hasSkip = false;
        // Scan for the appearance of ::, to mark a skip-format IPV6 string and adjust the partsSkipped
        // estimate.
        for (int i = 0; i < ipString.length() - 1; i++) {
            if (ipString.charAt(i) == IPV6_DELIMITER && ipString.charAt(i + 1) == IPV6_DELIMITER) {
                if (hasSkip) {
                    return null; // Can't have more than one ::
                }
                hasSkip = true;
                partsSkipped++; // :: means we skipped an extra part in between the two delimiters.
                if (i == 0) {
                    partsSkipped++; // Begins with ::, so we skipped the part preceding the first :
                }
                if (i == ipString.length() - 2) {
                    partsSkipped++; // Ends with ::, so we skipped the part after the last :
                }
            }
        }
        if (ipString.charAt(0) == IPV6_DELIMITER && ipString.charAt(1) != IPV6_DELIMITER) {
            return null; // ^: requires ^::
        }
        if (ipString.charAt(ipString.length() - 1) == IPV6_DELIMITER
                && ipString.charAt(ipString.length() - 2) != IPV6_DELIMITER) {
            return null; // :$ requires ::$
        }
        if (hasSkip && partsSkipped <= 0) {
            return null; // :: must expand to at least one '0'
        }
        if (!hasSkip && delimiterCount + 1 != IPV6_PART_COUNT) {
            return null; // Incorrect number of parts
        }

        ByteBuffer rawBytes = ByteBuffer.allocate(2 * IPV6_PART_COUNT);
        try {
            // Iterate through the parts of the ip string.
            // Invariant: start is always the beginning of a hextet, or the second ':' of the skip
            // sequence "::"
            int start = 0;
            if (ipString.charAt(0) == IPV6_DELIMITER) {
                start = 1;
            }
            while (start < ipString.length()) {
                int end = ipString.indexOf(IPV6_DELIMITER, start);
                if (end == -1) {
                    end = ipString.length();
                }
                if (ipString.charAt(start) == IPV6_DELIMITER) {
                    // expand zeroes
                    for (int i = 0; i < partsSkipped; i++) {
                        rawBytes.putShort((short) 0);
                    }

                } else {
                    rawBytes.putShort(parseHextet(ipString, start, end));
                }
                start = end + 1;
            }
        } catch (NumberFormatException ex) {
            return null;
        }
        return rawBytes.array();
    }

    @Nullable
    private static String convertDottedQuadToHex(String ipString) {
        int lastColon = ipString.lastIndexOf(':');
        String initialPart = ipString.substring(0, lastColon + 1);
        String dottedQuad = ipString.substring(lastColon + 1);
        byte[] quad = textToNumericFormatV4(dottedQuad);
        if (quad == null) {
            return null;
        }
        String penultimate = Integer.toHexString(((quad[0] & 0xff) << 8) | (quad[1] & 0xff));
        String ultimate = Integer.toHexString(((quad[2] & 0xff) << 8) | (quad[3] & 0xff));
        return initialPart + penultimate + ":" + ultimate;
    }

    private static byte parseOctet(String ipString, int start, int end) {
        // Note: we already verified that this string contains only hex digits, but the string may still
        // contain non-decimal characters.
        int length = end - start;
        if (length <= 0 || length > 3) {
            throw new NumberFormatException();
        }
        // Disallow leading zeroes, because no clear standard exists on
        // whether these should be interpreted as decimal or octal.
        if (length > 1 && ipString.charAt(start) == '0') {
            throw new NumberFormatException("IP address octal representation is not supported");
        }
        int octet = 0;
        for (int i = start; i < end; i++) {
            octet *= 10;
            int digit = Character.digit(ipString.charAt(i), 10);
            if (digit < 0) {
                throw new NumberFormatException();
            }
            octet += digit;
        }
        if (octet > 255) {
            throw new NumberFormatException();
        }
        return (byte) octet;
    }

    // Parse a hextet out of the ipString from start (inclusive) to end (exclusive)
    private static short parseHextet(String ipString, int start, int end) {
        // Note: we already verified that this string contains only hex digits.
        int length = end - start;
        if (length <= 0 || length > 4) {
            throw new NumberFormatException();
        }
        int hextet = 0;
        for (int i = start; i < end; i++) {
            hextet = hextet << 4;
            hextet |= Character.digit(ipString.charAt(i), 16);
        }
        return (short) hextet;
    }

    /**
     * Convert a byte array into an InetAddress.
     *
     * <p>{@link InetAddress#getByAddress} is documented as throwing a checked exception "if IP
     * address is of illegal length." We replace it with an unchecked exception, for use by callers
     * who already know that addr is an array of length 4 or 16.
     *
     * @param addr the raw 4-byte or 16-byte IP address in big-endian order
     * @return an InetAddress object created from the raw IP address
     */
    private static InetAddress bytesToInetAddress(byte[] addr) {
        try {
            return InetAddress.getByAddress(addr);
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
