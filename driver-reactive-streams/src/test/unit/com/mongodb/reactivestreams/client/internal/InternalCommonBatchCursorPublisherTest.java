/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.client.ListCollectionNamesIterableTest;
import com.mongodb.client.internal.InternalCommonIterableTest;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ChangeStreamPublisher;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.ListCollectionNamesPublisher;
import com.mongodb.reactivestreams.client.ListCollectionsPublisher;
import com.mongodb.reactivestreams.client.ListDatabasesPublisher;
import com.mongodb.reactivestreams.client.ListIndexesPublisher;
import com.mongodb.reactivestreams.client.ListSearchIndexesPublisher;
import com.mongodb.reactivestreams.client.gridfs.GridFSFindPublisher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.mongodb.client.internal.InternalCommonIterableTest.IS_PUBLIC_API;
import static com.mongodb.client.internal.InternalCommonIterableTest.assertEachApiInterfaceImplMustImplementInternalCommonInterface;
import static com.mongodb.client.internal.InternalCommonIterableTest.assertEachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonInterface;
import static com.mongodb.client.internal.InternalCommonIterableTest.assertEachInternalCommonInterfaceMethodMustBeInAtLeastTwoApiInterfaces;
import static com.mongodb.client.internal.InternalCommonIterableTest.findClasses;
import static com.mongodb.client.internal.InternalCommonIterableTest.methodSignatures;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class InternalCommonBatchCursorPublisherTest {
    /**
     * The relevant cursor publisher API interfaces. Implementations of most, but not all of them extend {@link BatchCursorPublisher}.
     */
    @SuppressWarnings("deprecation")
    private static final Set<Class<?>> API_INTERFACES = Stream.of(
                    AggregatePublisher.class,
                    ChangeStreamPublisher.class,
                    DistinctPublisher.class,
                    FindPublisher.class,
                    ListCollectionsPublisher.class,
                    ListCollectionNamesPublisher.class,
                    ListDatabasesPublisher.class,
                    ListIndexesPublisher.class,
                    ListSearchIndexesPublisher.class,
                    com.mongodb.reactivestreams.client.MapReducePublisher.class,
                    GridFSFindPublisher.class).
            collect(toCollection(() -> new TreeSet<>(comparing(Class::getName))));
    private static Path codeSourcePath;

    @BeforeAll
    static void beforeAll() throws Exception {
        codeSourcePath = Paths.get(BatchCursorPublisher.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    /**
     * The relevant cursor publisher API interfaces must be listed explicitly in {@link #API_INTERFACES}.
     * <p>
     * An interface is relevant if all the following is true:
     * <ol>
     *     <li>
     *     it is {@code public};</li>
     *     <li>
     *     it is not in an internal package;</li>
     *     <li>
     *     it is subinterface of {@link Publisher}, but not {@link Publisher} itself;</li>
     *     <li>
     *     it has a {@code public} method with the signature of a {@code public} method in {@link BatchCursorPublisher},
     *     excluding the {@link Publisher} and {@link Object} methods.</li>
     * </ol>
     */
    @Test
    void relevantApiInterfacesAreListedExplicitly() {
        Set<String> publisherMethodSignatures = methodSignatures(Publisher.class);
        Set<String> objectMethodSignatures = methodSignatures(Object.class);
        Set<String> cursorPublisherMarkingMethods = stream(BatchCursorPublisher.class.getMethods())
                .map(ListCollectionNamesIterableTest::signature)
                .filter(signature -> !publisherMethodSignatures.contains(signature) && !objectMethodSignatures.contains(signature))
                .collect(toCollection(TreeSet::new));
        assertEquals(API_INTERFACES, findClasses(codeSourcePath, IS_PUBLIC_API
                        .and(klass -> klass.isInterface())
                        .and(klass -> Publisher.class.isAssignableFrom(klass))
                        .and(klass -> methodSignatures(klass).stream().anyMatch(cursorPublisherMarkingMethods::contains))),
                "Either the explicit set of relevant interfaces or the code detecting them must be updated");
    }

    @Test
    void relevantApiInterfacesMatchTheSyncInterfaces() {
        Set<String> expectedApiInterfaceSimpleNames = InternalCommonIterableTest.API_INTERFACES.stream()
                .map(Class::getSimpleName)
                .map(syncApiInterfaceName -> syncApiInterfaceName.substring(0, syncApiInterfaceName.length() - "Iterable".length()) + "Publisher")
                .collect(toCollection(TreeSet::new));
        Set<String> actualApiInterfaceSimpleNames = API_INTERFACES.stream().map(Class::getSimpleName).collect(toCollection(TreeSet::new));
        assertEquals(expectedApiInterfaceSimpleNames, actualApiInterfaceSimpleNames,
                "The explicit set of relevant reactive interfaces must match that of the synchronous ones");
    }

    /**
     * Each cursor publisher API interface implementation must implement {@link InternalCommonBatchCursorPublisher}.
     */
    @Test
    void eachApiInterfaceImplMustImplementInternalCommonBatchCursorPublisher() {
        assertEachApiInterfaceImplMustImplementInternalCommonInterface(codeSourcePath, API_INTERFACES, InternalCommonBatchCursorPublisher.class);
    }

    /**
     * Each {@link InternalCommonBatchCursorPublisher} method must be in at least two cursor publisher API interfaces.
     */
    @Test
    void eachInternalCommonBatchCursorPublisherMethodMustBeInAtLeastTwoApiInterfaces() {
        assertEachInternalCommonInterfaceMethodMustBeInAtLeastTwoApiInterfaces(API_INTERFACES, InternalCommonBatchCursorPublisher.class);
    }

    /**
     * Each cursor publisher API interface method shared with another cursor publisher API interface must be in {@link InternalCommonBatchCursorPublisher}.
     */
    @Test
    void eachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonBatchCursorPublisher() {
        assertEachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonInterface(API_INTERFACES, InternalCommonBatchCursorPublisher.class);
    }
}
