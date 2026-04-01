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
 */
public final class ThreadDumpOnFailureExtension implements TestWatcher {

    private static final Logger LOGGER = Loggers.getLogger(ThreadDumpOnFailureExtension.class.getSimpleName());

    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause) {
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
