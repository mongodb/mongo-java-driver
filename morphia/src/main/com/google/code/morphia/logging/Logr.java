package com.google.code.morphia.logging;

import java.io.Serializable;

public interface Logr extends Serializable {
	
	public boolean isTraceEnabled();
	
	public void trace(String msg);
	
	public void trace(String format, Object... arg);
	
	public void trace(String msg, Throwable t);
	
	public boolean isDebugEnabled();
	
	public void debug(String msg);
	
	public void debug(String format, Object... arg);
	
	public void debug(String msg, Throwable t);
	
	public boolean isInfoEnabled();
	
	public void info(String msg);
	
	public void info(String format, Object... arg);
	
	public void info(String msg, Throwable t);
	
	public boolean isWarningEnabled();
	
	public void warning(String msg);
	
	public void warning(String format, Object... arg);
	
	public void warning(String msg, Throwable t);
	
	public boolean isErrorEnabled();
	
	public void error(String msg);
	
	public void error(String format, Object... arg);
	
	public void error(String msg, Throwable t);
	
}
