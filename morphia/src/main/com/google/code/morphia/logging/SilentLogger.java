/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package com.google.code.morphia.logging;

import java.io.Serializable;

/**
 * Silent logger; it doesn't do anything!
 */
public class SilentLogger implements Logr, Serializable {
    private static final long serialVersionUID = 1L;

    public boolean isTraceEnabled() {
        return false;
    }

    public void trace(final String msg) {
    }

    public void trace(final String format, final Object... arg) {
    }

    public void trace(final String msg, final Throwable t) {
    }

    public boolean isDebugEnabled() {
        return false;
    }

    public void debug(final String msg) {
    }

    public void debug(final String format, final Object... arg) {
    }

    public void debug(final String msg, final Throwable t) {
    }

    public boolean isInfoEnabled() {
        return false;
    }

    public void info(final String msg) {
    }

    public void info(final String format, final Object... arg) {
    }

    public void info(final String msg, final Throwable t) {
    }

    public boolean isWarningEnabled() {
        return false;
    }

    public void warning(final String msg) {
    }

    public void warning(final String format, final Object... arg) {
    }

    public void warning(final String msg, final Throwable t) {
    }

    public boolean isErrorEnabled() {
        return false;
    }

    public void error(final String msg) {
    }

    public void error(final String format, final Object... arg) {
    }

    public void error(final String msg, final Throwable t) {
    }
}
