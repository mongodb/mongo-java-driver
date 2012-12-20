package com.google.code.morphia.config;


public class MorphiaConfiguration {
	
	private boolean afterInitialization = false;
	
	private void assertBeforeInit() {
		if (afterInitialization) {
			throw new IllegalStateException("You cannot change this setting after starting Morphia.");
		}
	}
	
	void initialize() {
		afterInitialization = true;
	}
	
}
