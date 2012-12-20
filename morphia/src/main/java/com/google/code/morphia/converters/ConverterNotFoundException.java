/**
 * 
 */
package com.google.code.morphia.converters;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConverterNotFoundException extends RuntimeException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConverterNotFoundException(final String msg) {
		super(msg);
	}
}
