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
package com.mongodb.internal.graalvm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import static com.mongodb.ClusterFixture.getConnectionStringSystemPropertyOrDefault;

final class NativeImageApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(NativeImageApp.class);

    public static void main(final String[] args) {
        LOGGER.info("java.vendor={}, java.vm.name={}, java.version={}",
                System.getProperty("java.vendor"), System.getProperty("java.vm.name"), System.getProperty("java.version"));
        String[] arguments = new String[] {getConnectionStringSystemPropertyOrDefault()};
        List<Throwable> errors = Stream.<ThrowingRunnable>of(
                new ThrowingRunnable.Named(gridfs.GridFSTour.class,
                        () -> gridfs.GridFSTour.main(arguments)),
                new ThrowingRunnable.Named(documentation.CausalConsistencyExamples.class,
                        () -> documentation.CausalConsistencyExamples.main(arguments)),
                new ThrowingRunnable.Named(documentation.ChangeStreamSamples.class,
                        () -> documentation.ChangeStreamSamples.main(arguments)),
                new ThrowingRunnable.Named(tour.ClientSideEncryptionAutoEncryptionSettingsTour.class,
                        () -> tour.ClientSideEncryptionAutoEncryptionSettingsTour.main(arguments)),
                new ThrowingRunnable.Named(tour.ClientSideEncryptionExplicitEncryptionAndDecryptionTour.class,
                        () -> tour.ClientSideEncryptionExplicitEncryptionAndDecryptionTour.main(arguments)),
                new ThrowingRunnable.Named(tour.ClientSideEncryptionExplicitEncryptionOnlyTour.class,
                        () -> tour.ClientSideEncryptionExplicitEncryptionOnlyTour.main(arguments)),
                new ThrowingRunnable.Named(tour.ClientSideEncryptionQueryableEncryptionTour.class,
                        () -> tour.ClientSideEncryptionQueryableEncryptionTour.main(arguments)),
                new ThrowingRunnable.Named(tour.ClientSideEncryptionSimpleTour.class,
                        () -> tour.ClientSideEncryptionSimpleTour.main(arguments)),
                new ThrowingRunnable.Named(tour.Decimal128QuickTour.class,
                        () -> tour.Decimal128QuickTour.main(arguments)),
                new ThrowingRunnable.Named(tour.PojoQuickTour.class,
                        () -> tour.PojoQuickTour.main(arguments)),
                new ThrowingRunnable.Named(tour.QuickTour.class,
                        () -> tour.QuickTour.main(arguments)),
                new ThrowingRunnable.Named(tour.Decimal128LegacyAPIQuickTour.class,
                        () -> tour.Decimal128LegacyAPIQuickTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.gridfs.GridFSTour.class,
                        () -> reactivestreams.gridfs.GridFSTour.main(arguments)),
                // This tour is broken and hangs even when run by a JVM.
                // See https://jira.mongodb.org/browse/JAVA-5364.
                // new ThrowingRunnable.Named(reactivestreams.tour.ClientSideEncryptionAutoEncryptionSettingsTour.class,
                //         () -> reactivestreams.tour.ClientSideEncryptionAutoEncryptionSettingsTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.ClientSideEncryptionExplicitEncryptionAndDecryptionTour.class,
                        () -> reactivestreams.tour.ClientSideEncryptionExplicitEncryptionAndDecryptionTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.ClientSideEncryptionExplicitEncryptionOnlyTour.class,
                        () -> reactivestreams.tour.ClientSideEncryptionExplicitEncryptionOnlyTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.ClientSideEncryptionQueryableEncryptionTour.class,
                        () -> reactivestreams.tour.ClientSideEncryptionQueryableEncryptionTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.ClientSideEncryptionSimpleTour.class,
                        () -> reactivestreams.tour.ClientSideEncryptionSimpleTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.PojoQuickTour.class,
                        () -> reactivestreams.tour.PojoQuickTour.main(arguments)),
                new ThrowingRunnable.Named(reactivestreams.tour.QuickTour.class,
                        () -> reactivestreams.tour.QuickTour.main(arguments))
                ).map(ThrowingRunnable::runAndCatch)
                .filter(Objects::nonNull)
                .toList();
        if (!errors.isEmpty()) {
            AssertionError error = new AssertionError(String.format("%d %s failed",
                    errors.size(), errors.size() == 1 ? "application" : "applications"));
            errors.forEach(error::addSuppressed);
            throw error;
        }
    }

    private NativeImageApp() {
    }

    private interface ThrowingRunnable {
        void run() throws Exception;

        @Nullable
        default Throwable runAndCatch() {
            try {
                run();
            } catch (Exception | AssertionError e) {
                return e;
            }
            return null;
        }

        final class Named implements ThrowingRunnable {
            private final String name;
            private final ThrowingRunnable runnable;

            Named(final String name, final ThrowingRunnable runnable) {
                this.name = name;
                this.runnable = runnable;
            }

            Named(final Class<?> mainClass, final ThrowingRunnable runnable) {
                this(mainClass.getName(), runnable);
            }

            @Override
            public void run() throws Exception {
                runnable.run();
            }

            @Override
            @Nullable
            public Throwable runAndCatch() {
                Throwable t = runnable.runAndCatch();
                if (t != null) {
                    t = new AssertionError(name, t);
                }
                return t;
            }
        }
    }
}
