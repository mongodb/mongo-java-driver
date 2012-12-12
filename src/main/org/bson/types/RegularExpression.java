/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.bson.types;

/**
 * A Holder class for a BSON regular expression, so that we can delay compiling into a Pattern until necessary.
 */
public class RegularExpression {
    private final String pattern;
    private final String options;

    public RegularExpression(final String pattern, final String options) {
        this.pattern = pattern;
        this.options = options;
    }

    public RegularExpression(final String pattern) {
        this(pattern, "");
    }

    public String getPattern() {
        return pattern;
    }

    public String getOptions() {
        return options;
    }
}
