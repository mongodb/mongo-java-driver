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

package com.google.code.morphia.logging;

import java.io.Serializable;

public interface Logr extends Serializable {

    boolean isTraceEnabled();

    void trace(String msg);

    void trace(String format, Object... arg);

    void trace(String msg, Throwable t);

    boolean isDebugEnabled();

    void debug(String msg);

    void debug(String format, Object... arg);

    void debug(String msg, Throwable t);

    boolean isInfoEnabled();

    void info(String msg);

    void info(String format, Object... arg);

    void info(String msg, Throwable t);

    boolean isWarningEnabled();

    void warning(String msg);

    void warning(String format, Object... arg);

    void warning(String msg, Throwable t);

    boolean isErrorEnabled();

    void error(String msg);

    void error(String format, Object... arg);

    void error(String msg, Throwable t);

}
