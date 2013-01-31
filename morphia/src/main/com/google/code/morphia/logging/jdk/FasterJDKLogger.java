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

package com.google.code.morphia.logging.jdk;

import java.util.logging.Level;

@SuppressWarnings("rawtypes")
public class FasterJDKLogger extends FastestJDKLogger {
    private static final long serialVersionUID = 1L;

    public FasterJDKLogger(final Class c) {
        super(c);
    }

    private String getCallingMethod() {
        final StackTraceElement[] stack = (new Throwable()).getStackTrace();
        for (int j = 0; j < stack.length; j++) {
            final StackTraceElement ste = stack[j];
            if (className.equals(ste.getClassName())) {
                return ste.getMethodName();
            }
        }

        return "<method name unknown due to misused non-private logger>";
    }

    protected void log(final Level l, final String m, final Throwable t) {
        if (logger.isLoggable(l)) {
            logger.logp(l, className, getCallingMethod(), m, t);
        }
    }

    protected void log(final Level l, final String f, final Object... a) {
        if (logger.isLoggable(l)) {
            logger.logp(l, className, getCallingMethod(), f, a);
        }
    }
}
