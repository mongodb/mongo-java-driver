/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.util.ConcurrentModificationException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class LazyReferenceFetchingException extends
		ConcurrentModificationException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public LazyReferenceFetchingException(final String msg) {
		super(msg);
	}
}
