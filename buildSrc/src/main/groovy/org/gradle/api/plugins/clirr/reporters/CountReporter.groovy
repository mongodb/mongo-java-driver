/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.gradle.api.plugins.clirr.reporters

import net.sf.clirr.core.ApiDifference
import net.sf.clirr.core.Severity

class CountReporter implements Reporter {
    private int srcInfos = 0;
    private int srcWarnings = 0;
    private int srcErrors = 0;

    @Override
    void report(final Map<String, List<ApiDifference>> differences) {
        differences.each { key, value ->
            value.each { difference ->
                final Severity srcSeverity = difference.getSourceCompatibilitySeverity();
                if (Severity.ERROR.equals(srcSeverity)) {
                    srcErrors += 1;
                } else if (Severity.WARNING.equals(srcSeverity)) {
                    srcWarnings += 1;
                } else if (Severity.INFO.equals(srcSeverity)) {
                    srcInfos += 1;
                }
            }
        }
    }

    int getSrcInfos() {
        return srcInfos
    }

    int getSrcWarnings() {
        return srcWarnings
    }

    int getSrcErrors() {
        return srcErrors
    }
}
