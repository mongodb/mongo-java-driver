package com.mongodb;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class DBObjectMatchers {

    @Factory
    public static Matcher<DBObject> hasFields(final String[] fields) {
        return new HasFieldsMatcher(fields);
    }

    @Factory
    public static Matcher<DBObject> hasSubdocument(final DBObject subdocument) {
        return new HasSubdocumentMatcher(subdocument);
    }

    public static class HasFieldsMatcher extends TypeSafeMatcher<DBObject> {

        private String[] fieldNames;

        public HasFieldsMatcher(final String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        protected boolean matchesSafely(DBObject item) {
            for (String fieldName : fieldNames) {
                if (!item.containsField(fieldName)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText(" has fields ")
                    .appendValue(fieldNames);
        }
    }

    public static class HasSubdocumentMatcher extends TypeSafeMatcher<DBObject> {
        private final DBObject document;

        public HasSubdocumentMatcher(DBObject document) {
            this.document = document;
        }

        @Override
        protected boolean matchesSafely(DBObject item) {
            for (String key : document.keySet()) {
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
        public void describeTo(Description description) {
            description
                    .appendText(" has subdocument ")
                    .appendValue(document);
        }
    }
}
