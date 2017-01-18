/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.bson.codecs;

import org.bson.BsonReader;
import org.bson.BsonRegularExpression;
import org.bson.BsonWriter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A codec for {@code Pattern} instances.
 *
 * @since 3.0
 */
public class PatternCodec implements Codec<Pattern> {
    @Override
    public void encode(final BsonWriter writer, final Pattern value, final EncoderContext encoderContext) {
        writer.writeRegularExpression(new BsonRegularExpression(value.pattern(), getOptionsAsString(value)));
    }

    @Override
    public Pattern decode(final BsonReader reader, final DecoderContext decoderContext) {
        BsonRegularExpression regularExpression = reader.readRegularExpression();
        return Pattern.compile(regularExpression.getPattern(), getOptionsAsInt(regularExpression));
    }

    @Override
    public Class<Pattern> getEncoderClass() {
        return Pattern.class;
    }

    private static String getOptionsAsString(final Pattern pattern) {
        int flags = pattern.flags();
        StringBuilder buf = new StringBuilder();

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

    private static int getOptionsAsInt(final BsonRegularExpression regularExpression) {
        int optionsInt = 0;

        String optionsString = regularExpression.getOptions();

        if (optionsString == null || optionsString.length() == 0) {
            return optionsInt;
        }

        optionsString = optionsString.toLowerCase();

        for (int i = 0; i < optionsString.length(); i++) {
            RegexFlag flag = RegexFlag.getByCharacter(optionsString.charAt(i));
            if (flag != null) {
                optionsInt |= flag.javaFlag;
                if (flag.unsupported != null) {
                    // TODO: deal with logging
                    // warnUnsupportedRegex( flag.unsupported );
                }
            } else {
                // TODO: throw a better exception here
                throw new IllegalArgumentException("unrecognized flag [" + optionsString.charAt(i) + "] " + (int) optionsString.charAt(i));
            }
        }
        return optionsInt;
    }


    private static final int GLOBAL_FLAG = 256;

    private enum RegexFlag {
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
