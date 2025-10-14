/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.client.tracing;

import com.mongodb.lang.Nullable;
import io.micrometer.tracing.exporter.FinishedSpan;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.CLIENT_CONNECTION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.CURSOR_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_CONNECTION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SERVER_PORT;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.SESSION_ID;
import static com.mongodb.internal.tracing.MongodbObservation.LowCardinalityKeyNames.TRANSACTION_NUMBER;
import static org.bson.assertions.Assertions.notNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Represents a tree structure of spans, where each span can have nested spans as children.
 * This class provides methods to create a span tree from various sources and to validate the spans against expected values.
 */
public class SpanTree {
    private final List<SpanNode> roots = new ArrayList<>();

    /**
     * Creates a SpanTree from a BsonArray of spans.
     *
     * @param spans the BsonArray containing span documents
     * @return a SpanTree constructed from the provided spans
     */
    public static SpanTree from(final BsonArray spans) {
        SpanTree spanTree = new SpanTree();
        for (final BsonValue span : spans) {
            if (span.isDocument()) {
                final BsonDocument spanDoc = span.asDocument();
                final String name = spanDoc.getString("name").getValue();
                final SpanNode rootNode = new SpanNode(name);
                spanTree.roots.add(rootNode);

                if (spanDoc.containsKey("attributes")) {
                    rootNode.tags = spanDoc.getDocument("attributes");
                }

                if (spanDoc.containsKey("nested")) {
                    for (final BsonValue nestedSpan : spanDoc.getArray("nested")) {
                        addNestedSpans(rootNode, nestedSpan.asDocument());
                    }
                }
            }
        }

        return spanTree;
    }

    public static SpanTree from(final List<FinishedSpan> spans) {
        final SpanTree spanTree = new SpanTree();
        final Map<String, SpanNode> idToSpanNode = new HashMap<>();
        for (final FinishedSpan span : spans) {
            final SpanNode spanNode = new SpanNode(span.getName());
            for (final Map.Entry<String, String> tag : span.getTags().entrySet()) {
                // handle special case of session id (needs to be parsed into a BsonBinary)
                // this is needed because the SimpleTracer reports all the collected tags as strings
                if (tag.getKey().equals(SESSION_ID.asString())) {
                    spanNode.tags.append(tag.getKey(), new BsonDocument().append("id", new BsonBinary(UUID.fromString(tag.getValue()))));

                } else if (tag.getKey().equals(CURSOR_ID.asString())
                        || tag.getKey().equals(SERVER_PORT.asString())
                        || tag.getKey().equals(TRANSACTION_NUMBER.asString())
                        || tag.getKey().equals(CLIENT_CONNECTION_ID.asString())
                        || tag.getKey().equals(SERVER_CONNECTION_ID.asString())) {
                    spanNode.tags.append(tag.getKey(), new BsonInt64(Long.parseLong(tag.getValue())));
                } else {
                    spanNode.tags.append(tag.getKey(), new BsonString(tag.getValue()));
                }
            }
            idToSpanNode.put(span.getSpanId(), spanNode);
        }

        for (final FinishedSpan span : spans) {
            final String parentId = span.getParentId();
            final SpanNode node = idToSpanNode.get(span.getSpanId());

            if (parentId != null && !parentId.isEmpty() && idToSpanNode.containsKey(parentId)) {
                idToSpanNode.get(parentId).children.add(node);
            } else { // doesn't have a parent, so it is a root node
                spanTree.roots.add(node);
            }
        }
        return spanTree;
    }

    /**
     * Adds nested spans to the parent node based on the provided BsonDocument.
     * This method recursively adds child spans to the parent span node.
     *
     * @param parentNode the parent span node to which nested spans will be added
     * @param nestedSpan the BsonDocument representing a nested span
     */
    private static void addNestedSpans(final SpanNode parentNode, final BsonDocument nestedSpan) {
        final String name = nestedSpan.getString("name").getValue();
        final SpanNode childNode = new SpanNode(name, parentNode);

        if (nestedSpan.containsKey("attributes")) {
            childNode.tags = nestedSpan.getDocument("attributes");
        }

        if (nestedSpan.containsKey("nested")) {
            for (final BsonValue nested : nestedSpan.getArray("nested")) {
                addNestedSpans(childNode, nested.asDocument());
            }
        }
    }

    /**
     * Asserts that the reported spans are valid against the expected spans.
     * This method checks that the reported spans match the expected spans in terms of names, tags, and structure.
     *
     * @param reportedSpans    the SpanTree containing the reported spans
     * @param expectedSpans    the SpanTree containing the expected spans
     * @param valueMatcher     a BiConsumer to match values of tags between reported and expected spans
     * @param ignoreExtraSpans if true, allows reported spans to contain extra spans not present in expected spans
     */
    public static void assertValid(final SpanTree reportedSpans, final SpanTree expectedSpans,
            final BiConsumer<BsonValue, BsonValue> valueMatcher,
            final boolean ignoreExtraSpans) {
        if (ignoreExtraSpans) {
            // remove from the reported spans all the nodes that are not expected
            reportedSpans.roots.removeIf(node ->
                    expectedSpans.roots.stream().noneMatch(expectedNode -> expectedNode.getName().equalsIgnoreCase(node.getName()))
            );
        }

        // check that we have the same root spans
        if (reportedSpans.roots.size() != expectedSpans.roots.size()) {
            fail("The number of reported spans does not match expected spans size. "
                    + "Reported: " + reportedSpans.roots.size()
                    + ", Expected: " + expectedSpans.roots.size()
                    + " ignoreExtraSpans: " + ignoreExtraSpans);
        }

        for (int i = 0; i < reportedSpans.roots.size(); i++) {
            assertValid(reportedSpans.roots.get(i), expectedSpans.roots.get(i), valueMatcher);
        }
    }

    /**
     * Asserts that a reported span node is valid against an expected span node.
     * This method checks that the reported span's name, tags, and children match the expected span.
     *
     * @param reportedNode the reported span node to validate
     * @param expectedNode the expected span node to validate against
     * @param valueMatcher a BiConsumer to match values of tags between reported and expected spans
     */
    private static void assertValid(final SpanNode reportedNode, final SpanNode expectedNode,
            final BiConsumer<BsonValue, BsonValue> valueMatcher) {
        // Check that the span names match
        if (!reportedNode.getName().equalsIgnoreCase(expectedNode.getName())) {
            fail("Reported span name "
                    + reportedNode.getName()
                    + " does not match expected span name "
                    + expectedNode.getName());
        }

        valueMatcher.accept(expectedNode.tags, reportedNode.tags);

        // Spans should have the same number of children
        if (reportedNode.children.size() != expectedNode.children.size()) {
            fail("Reported span " + reportedNode.getName()
                    + " has " + reportedNode.children.size()
                    + " children, but expected " + expectedNode.children.size());
        }

        // For every reported child span make sure it is valid against the expected child span
        for (int i = 0; i < reportedNode.children.size(); i++) {
            assertValid(reportedNode.children.get(i), expectedNode.children.get(i), valueMatcher);
        }
    }

    @Override
    public String toString() {
        return "SpanTree{"
                + "roots=" + roots
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SpanTree spanTree = (SpanTree) o;
        return Objects.deepEquals(roots, spanTree.roots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(roots);
    }

    /**
     * Represents a node in the span tree, which can have nested child spans.
     * Each span node contains a name, tags, and a list of child span nodes.
     */
    public static class SpanNode {
        private final String name;
        private BsonDocument tags = new BsonDocument();
        private final List<SpanNode> children = new ArrayList<>();

        public SpanNode(final String name) {
            this.name = notNull("name", name);
        }

        public SpanNode(final String name, @Nullable final SpanNode parent) {
            this.name = notNull("name", name);
            if (parent != null) {
                parent.children.add(this);
            }
        }

        public String getName() {
            return name;
        }

        public List<SpanNode> getChildren() {
            return Collections.unmodifiableList(children);
        }

        @Override
        public String toString() {
            return "SpanNode{"
                    + "name='" + name + '\''
                    + ", tags=" + tags
                    + ", children=" + children
                    + '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SpanNode spanNode = (SpanNode) o;
            return name.equalsIgnoreCase(spanNode.name)
                    && Objects.equals(tags, spanNode.tags)
                    && Objects.equals(children, spanNode.children);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + tags.hashCode();
            result = 31 * result + children.hashCode();
            return result;
        }
    }
}
