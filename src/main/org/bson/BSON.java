// BSON.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.bson;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class BSON {

    static final Logger LOGGER = Logger.getLogger("org.bson.BSON");

    // ---- basics ----

    public static final byte EOO = 0;
    public static final byte NUMBER = 1;
    public static final byte STRING = 2;
    public static final byte OBJECT = 3;
    public static final byte ARRAY = 4;
    public static final byte BINARY = 5;
    public static final byte UNDEFINED = 6;
    public static final byte OID = 7;
    public static final byte BOOLEAN = 8;
    public static final byte DATE = 9;
    public static final byte NULL = 10;
    public static final byte REGEX = 11;
    public static final byte REF = 12;
    public static final byte CODE = 13;
    public static final byte SYMBOL = 14;
    public static final byte CODE_W_SCOPE = 15;
    public static final byte NUMBER_INT = 16;
    public static final byte TIMESTAMP = 17;
    public static final byte NUMBER_LONG = 18;

    public static final byte MINKEY = -1;
    public static final byte MAXKEY = 127;

    // --- binary types
    /*
       these are binary types
       so the format would look like
       <BINARY><name><BINARY_TYPE><...>
    */

    public static final byte B_GENERAL = 0;
    public static final byte B_FUNC = 1;
    public static final byte B_BINARY = 2;
    public static final byte B_UUID = 3;

    // ---- regular expression handling ----

    /**
     * Converts a string of regular expression flags from the database in Java regular expression flags.
     *
     * @param flags flags from database
     * @return the Java flags
     */
    public static int regexFlags(String flags) {
        int fint = 0;
        if (flags == null || flags.length() == 0) {
            return fint;
        }

        flags = flags.toLowerCase();

        for (int i = 0; i < flags.length(); i++) {
            RegexFlag flag = RegexFlag.getByCharacter(flags.charAt(i));
            if (flag != null) {
                fint |= flag.javaFlag;
                if (flag.unsupported != null) {
                    _warnUnsupportedRegex(flag.unsupported);
                }
            }
            else {
                throw new IllegalArgumentException("unrecognized flag [" + flags.charAt(i) + "] " + (int) flags.charAt(i));
            }
        }
        return fint;
    }

    public static int regexFlag(char c) {
        RegexFlag flag = RegexFlag.getByCharacter(c);
        if (flag == null) {
            throw new IllegalArgumentException("unrecognized flag [" + c + "]");
        }

        if (flag.unsupported != null) {
            _warnUnsupportedRegex(flag.unsupported);
            return 0;
        }

        return flag.javaFlag;
    }

    /**
     * Converts Java regular expression flags into a string of flags for the database
     *
     * @param flags Java flags
     * @return the flags for the database
     */
    public static String regexFlags(int flags) {
        StringBuilder buf = new StringBuilder();

        for (RegexFlag flag : RegexFlag.values()) {
            if ((flags & flag.javaFlag) > 0) {
                buf.append(flag.flagChar);
                flags -= flag.javaFlag;
            }
        }

        if (flags > 0) {
            throw new IllegalArgumentException("some flags could not be recognized.");
        }

        return buf.toString();
    }

    private static enum RegexFlag {
        CANON_EQ(Pattern.CANON_EQ, 'c', "Pattern.CANON_EQ"),
        UNIX_LINES(Pattern.UNIX_LINES, 'd', "Pattern.UNIX_LINES"),
        GLOBAL(GLOBAL_FLAG, 'g', null),
        CASE_INSENSITIVE(Pattern.CASE_INSENSITIVE, 'i', null),
        MULTILINE(Pattern.MULTILINE, 'm', null),
        DOTALL(Pattern.DOTALL, 's', "Pattern.DOTALL"),
        LITERAL(Pattern.LITERAL, 't', "Pattern.LITERAL"),
        UNICODE_CASE(Pattern.UNICODE_CASE, 'u', "Pattern.UNICODE_CASE"),
        COMMENTS(Pattern.COMMENTS, 'x', null);

        private static final Map<Character, RegexFlag> byCharacter = new HashMap<Character, RegexFlag>();

        static {
            for (RegexFlag flag : values()) {
                byCharacter.put(flag.flagChar, flag);
            }
        }

        public static RegexFlag getByCharacter(char ch) {
            return byCharacter.get(ch);
        }

        public final int javaFlag;
        public final char flagChar;
        public final String unsupported;

        RegexFlag(int f, char ch, String u) {
            javaFlag = f;
            flagChar = ch;
            unsupported = u;
        }
    }

    private static void _warnUnsupportedRegex(String flag) {
        LOGGER.info("flag " + flag + " not supported by db.");
    }

    private static final int GLOBAL_FLAG = 256;
}
