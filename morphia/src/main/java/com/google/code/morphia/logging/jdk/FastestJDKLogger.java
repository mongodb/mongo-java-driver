package com.google.code.morphia.logging.jdk;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.code.morphia.logging.Logr;

@SuppressWarnings("rawtypes")
public class FastestJDKLogger implements Logr {
	private static final long serialVersionUID = 1L;
	protected final Logger logger;
	protected final String className;
	
	public FastestJDKLogger(Class c) {
		className = c.getName();
		logger = Logger.getLogger(className);
	}
	
	public boolean isTraceEnabled() {
		return logger.isLoggable(Level.FINER);
	}
	
	public void trace(String msg) {
		log(Level.FINER, msg);
	}
	
	public void trace(String format, Object... arg) {
		log(Level.FINER, format, arg);
	}
	
	public void trace(String msg, Throwable t) {
		log(Level.FINER, msg, t);
	}
	
	public boolean isDebugEnabled() {
		return logger.isLoggable(Level.FINE);
	}
	
	public void debug(String msg) {
		log(Level.FINE, msg);
	}
	
	public void debug(String format, Object... arg) {
		log(Level.FINE, format, arg);
	}
	
	public void debug(String msg, Throwable t) {
		log(Level.FINE, msg, t);
		
	}
	
	public boolean isInfoEnabled() {
		return logger.isLoggable(Level.INFO);
	}
	
	public void info(String msg) {
		log(Level.INFO, msg);
	}
	
	public void info(String format, Object... arg) {
		log(Level.INFO, format, arg);
	}
	
	public void info(String msg, Throwable t) {
		log(Level.INFO, msg, t);
	}
	
	public boolean isWarningEnabled() {
		return logger.isLoggable(Level.WARNING);
	}
	
	public void warning(String msg) {
		log(Level.WARNING, msg);
	}
	
	public void warning(String format, Object... arg) {
		log(Level.WARNING, format, arg);
	}
	
	public void warning(String msg, Throwable t) {
		log(Level.WARNING, msg, t);
	}
	
	public boolean isErrorEnabled() {
		return logger.isLoggable(Level.SEVERE);
	}
	
	public void error(String msg) {
		log(Level.SEVERE, msg);
		
	}
	
	public void error(String format, Object... arg) {
		log(Level.SEVERE, format, arg);
		
	}
	
	public void error(String msg, Throwable t) {
		log(Level.SEVERE, msg, t);
	}
	
	protected void log(Level l, String m, Throwable t) {
		if (logger.isLoggable(l)) {
			logger.logp(l, className, null, m, t);
		}
	}
	
	protected void log(Level l, String f, Object... a) {
		if (logger.isLoggable(l)) {
			logger.logp(l, className, null, f, a);
		}
	}
	
	
}
