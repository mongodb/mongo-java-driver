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
