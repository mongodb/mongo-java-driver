package com.google.code.morphia.logging.jdk;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.LogrFactory;

public class JDKLoggerFactory implements LogrFactory {
	
	public Logr get(Class<?> c) {
		return new JDKLogger(c);
	}
	
}
