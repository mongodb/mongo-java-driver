package com.google.code.morphia.logging;

import java.io.Serializable;

/** Silent logger; it doesn't do anything! */
public class SilentLogger implements Logr, Serializable{
	private static final long serialVersionUID = 1L;

	public boolean isTraceEnabled() { return false; }
	public void trace(String msg) {}
	public void trace(String format, Object... arg) {}
	public void trace(String msg, Throwable t) {}
	public boolean isDebugEnabled() { return false; }
	public void debug(String msg) {}
	public void debug(String format, Object... arg) {}
	public void debug(String msg, Throwable t) {}
	public boolean isInfoEnabled() {return false;}
	public void info(String msg) {}
	public void info(String format, Object... arg) {}
	public void info(String msg, Throwable t) {}
	public boolean isWarningEnabled() { return false; }
	public void warning(String msg) {	}
	public void warning(String format, Object... arg) {	}
	public void warning(String msg, Throwable t) {	}
	public boolean isErrorEnabled() { return false; }
	public void error(String msg) {	}
	public void error(String format, Object... arg) {}
	public void error(String msg, Throwable t) {}
}
