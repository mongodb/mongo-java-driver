package com.mongodb.client.unified;

import com.mongodb.assertions.Assertions;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class UnifiedTestSkips {

    public static TestDef testDef(final String dir, final String file, final String test) {
        return new TestDef(dir, file, test);
    }

    public static class TestDef {
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

    public static class Skip {
        private final TestDef testDef;
        private final String reason;

        private Skip(final TestDef testDef, final String reason) {
            this.testDef = testDef;
            this.reason = reason;
        }

        /**
         * All tests in file under dir skipped.
         */
        public Skip file(final String dir, final String file) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && file.equals(testDef.file);
            assumeFalse(match, reason);
            return this;
        }

        /**
         * Test skipped if dir, file, and test match.
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
         */
        public Skip testContains(final String dir, final String fragment) {
            boolean match = ("unified-test-format/" + dir).equals(testDef.dir)
                    && testDef.test.contains(fragment);
            assumeFalse(match, reason);
            return this;
        }
    }
}
