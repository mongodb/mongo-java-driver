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

package com.mongodb.client.internal;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListCollectionNamesIterable;
import com.mongodb.client.ListCollectionNamesIterableTest;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.ListSearchIndexesIterable;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.GridFSFindIterable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.String.format;
import static java.nio.file.Files.isDirectory;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class InternalCommonIterableTest {
    /**
     * The relevant iterable API interfaces. They are all subinterfaces of {@link MongoIterable}.
     */
    @SuppressWarnings("deprecation")
    public static final Set<Class<?>> API_INTERFACES = Stream.of(
            AggregateIterable.class,
            ChangeStreamIterable.class,
            DistinctIterable.class,
            FindIterable.class,
            ListCollectionsIterable.class,
            ListCollectionNamesIterable.class,
            ListDatabasesIterable.class,
            ListIndexesIterable.class,
            ListSearchIndexesIterable.class,
            com.mongodb.client.MapReduceIterable.class,
            GridFSFindIterable.class).
            collect(toCollection(() -> new TreeSet<>(comparing(Class::getName))));
    public static final Predicate<Class<?>> IS_PUBLIC_API = klass -> Modifier.isPublic(klass.getModifiers())
            && !asList(klass.getPackage().getName().split("\\.")).contains("internal");
    private static final ClassLoader CLASS_LOADER = MongoIterable.class.getClassLoader();
    private static Path codeSourcePath;

    @BeforeAll
    static void beforeAll() throws Exception {
        codeSourcePath = Paths.get(MongoIterable.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    /**
     * The relevant iterable API interfaces must be listed explicitly in {@link #API_INTERFACES}.
     * <p>
     * An interface is relevant if all the following is true:
     * <ol>
     *     <li>
     *     it is {@code public};</li>
     *     <li>
     *     it is not in an internal package;</li>
     *     <li>
     *     it is subinterface of {@link MongoIterable}, but not {@link MongoIterable} itself.</li>
     * </ol>
     */
    @Test
    void relevantApiInterfacesAreListedExplicitly() {
        assertEquals(API_INTERFACES, findClasses(codeSourcePath, IS_PUBLIC_API
                        .and(klass -> klass.isInterface())
                        .and(klass -> MongoIterable.class.isAssignableFrom(klass))
                        .and(klass -> !klass.equals(MongoIterable.class))),
                "Either the explicit set of relevant interfaces or the code detecting them must be updated");
    }

    /**
     * Each iterable API interface implementation must implement {@link InternalCommonIterable}.
     */
    @Test
    void eachApiInterfaceImplMustImplementInternalCommonIterable() {
        assertEachApiInterfaceImplMustImplementInternalCommonInterface(codeSourcePath, API_INTERFACES, InternalCommonIterable.class);
    }

    public static void assertEachApiInterfaceImplMustImplementInternalCommonInterface(
            final Path codeSourceDirectory, final Set<Class<?>> apiInterfaces, final Class<?> internalCommonInterface) {
        Predicate<Class<?>> isImplClass = klass -> !klass.isInterface()
                && apiInterfaces.stream().anyMatch(apiInterface -> apiInterface.isAssignableFrom(klass));
        Set<Class<?>> implClasses = findClasses(codeSourceDirectory, isImplClass);
        apiInterfaces.forEach(apiInterface -> assertTrue(
                implClasses.stream().anyMatch(implClass -> apiInterface.isAssignableFrom(implClass)),
                () -> format("No implementing classes were found for %s", apiInterface)));
        implClasses.forEach(implClass ->
                assertTrue(internalCommonInterface.isAssignableFrom(implClass), () ->
                        format("%s implementing an API interface must implement %s", implClass, internalCommonInterface)));
    }

    /**
     * Each {@link InternalCommonIterable} method must be in at least two iterable API interfaces.
     */
    @Test
    void eachInternalCommonIterableMethodMustBeInAtLeastTwoApiInterfaces() {
        assertEachInternalCommonInterfaceMethodMustBeInAtLeastTwoApiInterfaces(API_INTERFACES, InternalCommonIterable.class);
    }

    public static void assertEachInternalCommonInterfaceMethodMustBeInAtLeastTwoApiInterfaces(
            final Set<Class<?>> apiInterfaces, final Class<?> internalCommonInterface) {
        methodSignatures(internalCommonInterface).forEach(method -> {
            Set<Class<?>> apiInterfacesWithMethod = apiInterfaces.stream()
                    .filter(apiInterface -> methodSignatures(apiInterface).contains(method))
                    .collect(toCollection(() -> new TreeSet<>(comparing(Class::getName))));
            assertTrue(apiInterfacesWithMethod.size() >= 2, () ->
                    format("Method %s is in %d API interfaces %s, remove the method from %s",
                            method, apiInterfacesWithMethod.size(), apiInterfacesWithMethod, internalCommonInterface));
        });
    }

    /**
     * Each iterable API interface method shared with another iterable API interface must be in {@link InternalCommonIterable}.
     */
    @Test
    void eachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonIterable() {
        assertEachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonInterface(API_INTERFACES,
                InternalCommonIterable.class);
    }

    public static void assertEachApiInterfaceMethodSharedWithAnotherApiInterfaceMustBeInInternalCommonInterface(
            final Set<Class<?>> apiInterfaces, final Class<?> internalCommonInterface) {
        Set<String> expectedMethods = methodSignatures(internalCommonInterface);
        apiInterfaces.forEach(apiInterface -> {
            methodSignatures(apiInterface).forEach(method -> {
                Set<Class<?>> apiInterfacesWithMethod = apiInterfaces.stream()
                        .filter(maybeSameApiInterface -> methodSignatures(maybeSameApiInterface).contains(method))
                        .collect(toCollection(() -> new TreeSet<>(comparing(Class::getName))));
                if (apiInterfacesWithMethod.size() >= 2) {
                    assertTrue(expectedMethods.contains(method), () ->
                            format("Method %s is in multiple API interfaces %s, add the method to %s",
                                    method, apiInterfacesWithMethod, internalCommonInterface));
                }
            });
        });
    }

    public static Set<Class<?>> findClasses(final Path codeSourceDirectory, final Predicate<Class<?>> filter) {
        com.mongodb.assertions.Assertions.assertTrue(isDirectory(codeSourceDirectory), format("%s is not a directory", codeSourceDirectory));
        try (Stream<Path> paths = Files.walk(codeSourceDirectory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".class"))
                    .filter(pathOfClass ->
                            !pathOfClass.getFileName().equals(Paths.get("package-info.class"))
                            && !pathOfClass.getFileName().equals(Paths.get("module-info.class")))
                    .map(pathOfClass -> codeSourceDirectory.relativize(pathOfClass))
                    .map(relativePathOfClass -> {
                        String dottedPath = StreamSupport.stream(relativePathOfClass.spliterator(), false)
                                .map(Path::toString)
                                .collect(joining("."));
                        return dottedPath.substring(0, dottedPath.length() - ".class".length());
                    })
                    .map(classBinaryName -> {
                        try {
                            return Class.forName(classBinaryName, false, CLASS_LOADER);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .filter(filter)
                    .collect(toCollection(() -> new TreeSet<>(comparing(Class::getName))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<String> methodSignatures(final Class<?> klass) {
        return stream(klass.getMethods())
                .map(ListCollectionNamesIterableTest::signature)
                // `TreeSet` results in the alphabetic order of signatures in `toString`, improving assertion messages
                .collect(toCollection(TreeSet::new));
    }
}
