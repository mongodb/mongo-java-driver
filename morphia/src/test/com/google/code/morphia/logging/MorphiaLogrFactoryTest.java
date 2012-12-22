/**
 * 
 */
package com.google.code.morphia.logging;

import junit.framework.Assert;

import org.junit.Test;

import com.google.code.morphia.TestBase;

/**
 * @author us@thomas-daily.de
 * 
 */
public class MorphiaLogrFactoryTest extends TestBase {

	static {
		
		MorphiaLoggerFactory.reset();
		MorphiaLoggerFactory.registerLogger(TestLoggerFactory.class);
	}

	public MorphiaLogrFactoryTest() {

	}
	
	@Test
	public void testChoice() throws Exception {
		Logr logr = MorphiaLoggerFactory.get(Object.class);
		String className = logr.getClass().getName();
		Assert.assertTrue(className.startsWith(TestLoggerFactory.class.getName() + "$"));
	}
	
	@Override
	public void tearDown() {
		MorphiaLoggerFactory.reset();
		super.tearDown();
	}

	static class TestLoggerFactory implements LogrFactory {
		public Logr get(Class<?> c) {
			return new Logr() {
				
				public void warning(String msg, Throwable t) {
					
				}
				
				public void warning(String format, Object... arg) {
					
				}
				
				public void warning(String msg) {
					
				}
				
				public void trace(String msg, Throwable t) {
					
				}
				
				public void trace(String format, Object... arg) {
					
				}
				
				public void trace(String msg) {
					
				}
				
				public boolean isWarningEnabled() {

					return false;
				}
				
				public boolean isTraceEnabled() {

					return false;
				}
				
				public boolean isInfoEnabled() {

					return false;
				}
				
				public boolean isErrorEnabled() {

					return false;
				}
				
				public boolean isDebugEnabled() {

					return false;
				}
				
				public void info(String msg, Throwable t) {
					
				}
				
				public void info(String format, Object... arg) {
					
				}
				
				public void info(String msg) {
					
				}
				
				public void error(String msg, Throwable t) {
					
				}
				
				public void error(String format, Object... arg) {
					
				}
				
				public void error(String msg) {
					
				}
				
				public void debug(String msg, Throwable t) {
					
				}
				
				public void debug(String format, Object... arg) {
					
				}
				
				public void debug(String msg) {
					
				}
			};
		}
		
	}
}
