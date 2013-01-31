/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.serialization.serializers;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.RegularExpression;
import org.mongodb.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class PatternSerializer implements Serializer<Pattern> {
    @Override
    public void serialize(final BSONWriter bsonWriter, final Pattern value) {
        bsonWriter.writeRegularExpression(new RegularExpression(value.pattern(), getOptionsAsString(value)));
    }

    @Override
    public Pattern deserialize(final BSONReader reader) {
        final RegularExpression regularExpression = reader.readRegularExpression();
        return Pattern.compile(regularExpression.getPattern(), getOptionsAsInt(regularExpression));
    }

    @Override
    public Class<Pattern> getSerializationClass() {
        return Pattern.class;
    }

    public String getOptionsAsString(final Pattern pattern) {
        int flags = pattern.flags();
        final StringBuilder buf = new StringBuilder();

        for (final RegexFlag flag : RegexFlag.values()) {
            if ((pattern.flags() & flag.javaFlag) > 0) {
                buf.append(flag.flagChar);
                flags -= flag.javaFlag;
            }
        }

        if (flags > 0) {
            throw new IllegalArgumentException("some flags could not be recognized.");
        }

        return buf.toString();
    }

    public static int getOptionsAsInt(final RegularExpression regularExpression) {
        int optionsInt = 0;

        String optionsString = regularExpression.getOptions();

        if (optionsString == null || optionsString.length() == 0) {
            return optionsInt;
        }

        optionsString = optionsString.toLowerCase();

        for (int i = 0; i < optionsString.length(); i++) {
            final RegexFlag flag = RegexFlag.getByCharacter(optionsString.charAt(i));
            if (flag != null) {
                optionsInt |= flag.javaFlag;
                //CHECKSTYLE:OFF
                if (flag.unsupported != null) {
                    // TODO: deal with logging
                    // warnUnsupportedRegex( flag.unsupported );
                }
                //CHECKSTYLE:ON
            }
            else {
                // TODO: throw a better exception here
                throw new IllegalArgumentException("unrecognized flag [" + optionsString.charAt(i) + "] " + (int) optionsString.charAt(i));
            }
        }
        return optionsInt;
    }


    private static final int GLOBAL_FLAG = 256;

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

        private static final Map<Character, RegexFlag> BY_CHARACTER = new HashMap<Character, RegexFlag>();

        private final int javaFlag;
        private final char flagChar;
        private final String unsupported;

        static {
            for (final RegexFlag flag : values()) {
                BY_CHARACTER.put(flag.flagChar, flag);
            }
        }

        public static RegexFlag getByCharacter(final char ch) {
            return BY_CHARACTER.get(ch);
        }

        RegexFlag(final int f, final char ch, final String u) {
            javaFlag = f;
            flagChar = ch;
            unsupported = u;
        }
    }

}
