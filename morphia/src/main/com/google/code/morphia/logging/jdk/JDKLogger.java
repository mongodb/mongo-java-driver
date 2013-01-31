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

import com.google.code.morphia.logging.Logr;

import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("rawtypes")
public class JDKLogger implements Logr {
    private static final long serialVersionUID = 1L;
    private final Logger logger;

    public JDKLogger(final Class c) {
        logger = Logger.getLogger(c.getName());
    }

    public boolean isTraceEnabled() {
        return logger.isLoggable(Level.FINER);
    }

    public void trace(final String msg) {
        log(Level.FINER, msg);
    }

    public void trace(final String format, final Object... arg) {
        log(Level.FINER, format, arg);
    }

    public void trace(final String msg, final Throwable t) {
        log(Level.FINER, msg, t);
    }

    public boolean isDebugEnabled() {
        return logger.isLoggable(Level.FINE);
    }

    public void debug(final String msg) {
        log(Level.FINE, msg);
    }

    public void debug(final String format, final Object... arg) {
        log(Level.FINE, format, arg);
    }

    public void debug(final String msg, final Throwable t) {
        log(Level.FINE, msg, t);

    }

    public boolean isInfoEnabled() {
        return logger.isLoggable(Level.INFO);
    }

    public void info(final String msg) {
        log(Level.INFO, msg);
    }

    public void info(final String format, final Object... arg) {
        log(Level.INFO, format, arg);
    }

    public void info(final String msg, final Throwable t) {
        log(Level.INFO, msg, t);
    }

    public boolean isWarningEnabled() {
        return logger.isLoggable(Level.WARNING);
    }

    public void warning(final String msg) {
        log(Level.WARNING, msg);
    }

    public void warning(final String format, final Object... arg) {
        log(Level.WARNING, format, arg);
    }

    public void warning(final String msg, final Throwable t) {
        log(Level.WARNING, msg, t);
    }

    public boolean isErrorEnabled() {
        return logger.isLoggable(Level.SEVERE);
    }

    public void error(final String msg) {
        log(Level.SEVERE, msg);

    }

    public void error(final String format, final Object... arg) {
        log(Level.SEVERE, format, arg);

    }

    public void error(final String msg, final Throwable t) {
        log(Level.SEVERE, msg, t);
    }

    protected void log(final Level l, final String m, final Throwable t) {
        final String[] callerInfo = getCaller(new Throwable());
        logger.logp(l, callerInfo[0], callerInfo[1], m, t);
    }

    protected void log(final Level l, final String f, final Object... a) {
        final String[] callerInfo = getCaller(new Throwable());
        logger.logp(l, callerInfo[0], callerInfo[1], f, a);
    }

    /**
     * returns an array (class, method) of the caller before our logger class in the stack
     */
    protected String[] getCaller(final Throwable t) {
        final StackTraceElement[] stack = (new Throwable()).getStackTrace();
        final String loggerClassname = getClass().getName();

        String callerName = "";
        String callerMethod = "";

        int i = 0;
        while (i < stack.length) {
            final StackTraceElement ste = stack[i];
            final String fc = ste.getClassName();
            if (fc.equals(loggerClassname)) {
                break;
            }

            i++;
        }

        //skip an extra frame... we call ourselves
        i++;

        while (i < stack.length) {
            final StackTraceElement ste = stack[i];
            final String fc = ste.getClassName();
            if (!fc.equals(loggerClassname)) {
                callerMethod = ste.getMethodName();
                callerName = fc;
                return new String[]{callerName, callerMethod};
            }
            i++;
        }
        return new String[]{"", ""};
    }
}
