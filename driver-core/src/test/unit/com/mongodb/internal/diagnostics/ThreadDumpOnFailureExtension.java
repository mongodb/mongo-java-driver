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

package com.mongodb.internal.diagnostics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;


import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;


/**
 * A JUnit 5 extension that prints a thread dump to the log when a test fails.
 *
 * <p>The extension is auto-detected on every test run, but is inert unless explicitly enabled via the
 * {@value #ENABLED_PROPERTY} system property. This keeps local runs quiet while allowing Evergreen (which sets the
 * property) — or a developer who opts in with {@code -Dorg.mongodb.test.diagnostics.thread.dump.enabled=true} — to
 * capture thread dumps for failing tests.
 */
public final class ThreadDumpOnFailureExtension implements TestWatcher {

    static final String ENABLED_PROPERTY = "org.mongodb.test.diagnostics.thread.dump.enabled";

    private static final Logger LOGGER = Loggers.getLogger(ThreadDumpOnFailureExtension.class.getSimpleName());

    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause) {
        if (!Boolean.getBoolean(ENABLED_PROPERTY)) {
            return;
        }
        String testName = context.getDisplayName();
        String threadDump = getAllThreadsDump();
        LOGGER.error("Test failed: " + testName + "\nThread dump:\n" + threadDump);
    }

    private static String getAllThreadsDump() {
        try {
            final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(
                    threadMXBean.isObjectMonitorUsageSupported(),
                    threadMXBean.isSynchronizerUsageSupported()
            );
            StringBuilder sb = new StringBuilder(1024);
            for (ThreadInfo info : threadInfos) {
                sb.append(info);
            }
            return sb.toString();
        } catch (final SecurityException exc) {
            return "Unable to get thread dump due to security manager restrictions: " + exc.getMessage();
        }
    }
}
