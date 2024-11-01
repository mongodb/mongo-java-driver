/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.unified;

import com.mongodb.assertions.Assertions;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public final class UnifiedTestSkips {

    private UnifiedTestSkips() {}

    public static TestDef testDef(final String dir, final String file, final String test) {
        return new TestDef(dir, file, test);
    }

    public static final class TestDef {
        private final String dir;
        private final String file;
        private final String test;

        private TestDef(final String dir, final String file, final String test) {
            this.dir = dir;
            this.file = file;
            this.test = test;
        }

        /**
         * Test is skipped because it is pending implementation, and there is
         * a Jira ticket tracking this which has more information.
         *
         * @param skip reason for skipping the test; must start with a Jira URL
         */
        public Skip skipJira(final String skip) {
            Assertions.assertTrue(skip.startsWith("https://jira.mongodb.org/browse/JAVA-"));
            return new Skip(this, skip);
        }

        /**
         * Test is skipped because the feature under test was deprecated, and
         * was removed in the Java driver.
         *
         * @param skip reason for skipping the test
         */
        public Skip skipDeprecated(final String skip) {
            return new Skip(this, skip);
        }

        /**
         * Test is skipped because the Java driver cannot comply with the spec.
         *
         * @param skip reason for skipping the test
         */
        public Skip skipNoncompliant(final String skip) {
            return new Skip(this, skip);
        }
    }

    public static final class Skip {
        private final TestDef testDef;
        private final String reason;

        private Skip(final TestDef testDef, final String reason) {
            this.testDef = testDef;
            this.reason = reason;
        }

        /**
         * All tests in file under dir skipped.
         * @param dir the directory name
         * @param file the test file's "description" field
         * @return this
         */
        public Skip file(final String dir, final String file) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && file.equals(testDef.file);
            assumeFalse(match, reason);
            return this;
        }

        /**
         * Test skipped if dir, file, and test match.
         * @param dir the directory name
         * @param file the test file's "description" field
         * @param test the individual test's "description" field
         * @return this
         */
        public Skip test(final String dir, final String file, final String test) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && file.equals(testDef.file)
                    && test.equals(testDef.test);
            assumeFalse(match, reason);
            return this;
        }

        /**
         * Test skipped if the test description contains the fragment as a substring
         * Avoid using this, except during development.
         * @param dir the directory name
         * @param fragment the substring to check in the test "description" field
         * @return this
         */
        public Skip testContains(final String dir, final String fragment) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && testDef.test.contains(fragment);
            assumeFalse(match, reason);
            return this;
        }
    }
}
