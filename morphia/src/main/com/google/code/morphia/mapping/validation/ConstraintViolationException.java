package com.google.code.morphia.mapping.validation;

/**
 * 
 */

import java.util.Collection;

import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConstraintViolationException extends MappingException {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ConstraintViolationException(final Collection<ConstraintViolation> ve) {
		super(createString(ve.toArray(new ConstraintViolation[ve.size()])));
	}
	
	public ConstraintViolationException(final ConstraintViolation... ve) {
		super(createString(ve));
	}
	
	private static final String createString(final ConstraintViolation[] ve) {
		final StringBuffer sb = new StringBuffer(128);
		sb.append("Number of violations: " + ve.length + "\n");
		for (final ConstraintViolation validationError : ve) {
			sb.append(validationError.render());
		}
		final String message = sb.toString();
		return message;
	}
}
