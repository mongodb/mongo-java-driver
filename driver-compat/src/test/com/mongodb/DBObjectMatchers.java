package com.mongodb;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Set;

public class DBObjectMatchers {

    @Factory
    public static Matcher<DBObject> hasFields(Set<Map.Entry<String, Object>> fields) {
        return new HasFieldsMatcher(fields);
    }

    public static class HasFieldsMatcher extends TypeSafeMatcher<DBObject> {

        private Set<Map.Entry<String, Object>> entries;

        public HasFieldsMatcher(final Set<Map.Entry<String, Object>> entries) {
            this.entries = entries;
        }

        @Override
        protected boolean matchesSafely(DBObject item) {
            for (Map.Entry<String, Object> entry : entries) {
                if (entry.getValue() != null && item.get(entry.getKey()) == null) {
                    return false;
                }
                if (entry.getValue() != null && !entry.getValue().equals(item.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText(" has fields ")
                    .appendValue(entries);
        }
    }
}
