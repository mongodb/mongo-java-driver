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

package com.mongodb;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public final class DBObjectMatchers {

    private DBObjectMatchers() { }

    @Factory
    public static Matcher<DBObject> hasFields(final String[] fields) {
        return new HasFieldsMatcher(fields);
    }

    @Factory
    public static Matcher<DBObject> hasSubdocument(final DBObject subdocument) {
        return new HasSubdocumentMatcher(subdocument);
    }

    public static class HasFieldsMatcher extends TypeSafeMatcher<DBObject> {

        private final String[] fieldNames;

        public HasFieldsMatcher(final String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        protected boolean matchesSafely(final DBObject item) {
            for (final String fieldName : fieldNames) {
                if (!item.containsField(fieldName)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(" has fields ")
                       .appendValue(fieldNames);
        }
    }

    public static class HasSubdocumentMatcher extends TypeSafeMatcher<DBObject> {
        private final DBObject document;

        public HasSubdocumentMatcher(final DBObject document) {
            this.document = document;
        }

        @Override
        protected boolean matchesSafely(final DBObject item) {
            for (final String key : document.keySet()) {
                if (document.get(key) != null && item.get(key) == null) {
                    return false;
                }
                if (document.get(key) != null && !document.get(key).equals(item.get(key))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(" has subdocument ")
                       .appendValue(document);
        }
    }
}
