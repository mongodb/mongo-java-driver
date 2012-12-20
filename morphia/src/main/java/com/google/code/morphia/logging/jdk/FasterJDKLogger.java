package com.google.code.morphia.logging.jdk;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.code.morphia.logging.Logr;

@SuppressWarnings("rawtypes")
public class FasterJDKLogger extends FastestJDKLogger {
	private static final long serialVersionUID = 1L;
	public FasterJDKLogger(Class c) {
		super(c);
	}

	private String getCallingMethod() {
			StackTraceElement stack[] = (new Throwable()).getStackTrace();
			for (int j = 0; j < stack.length; j++) {
				StackTraceElement ste = stack[j];
				if(className.equals(ste.getClassName()))
					return ste.getMethodName();
			}
			
			return "<method name unknown due to misused non-private logger>";
	}
	
	protected void log(Level l, String m, Throwable t) {
		if (logger.isLoggable(l)) {
			logger.logp(l, className, getCallingMethod(), m, t);
		}
	}
	
	protected void log(Level l, String f, Object... a) {
		if (logger.isLoggable(l)) {
			logger.logp(l, className, getCallingMethod(), f, a);
		}
	}
}
