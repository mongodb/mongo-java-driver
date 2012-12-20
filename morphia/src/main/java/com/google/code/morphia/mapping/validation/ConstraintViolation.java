package com.google.code.morphia.mapping.validation;

import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class ConstraintViolation {
	public enum Level {
		MINOR, INFO, WARNING, SEVERE, FATAL;
	}

	private final MappedClass clazz;
	private MappedField field = null;
	private Class<? extends ClassConstraint> validator;
	private final String message;
	private final Level level;

	public ConstraintViolation(Level level, MappedClass clazz, MappedField field, Class<? extends ClassConstraint> validator, String message) {
		this(level, clazz, validator, message);
		this.field = field;
	}
	
	public ConstraintViolation(Level level, MappedClass clazz, Class<? extends ClassConstraint> validator, String message) {
		this.level = level;
		this.clazz = clazz;
		this.message = message;
		this.validator = validator;
	}
	
	public String render() {
		return String.format("%s complained about %s : %s",
				validator.getSimpleName(),
				getPrefix(),
				message);
	}

	public Level getLevel() {
		return level;
	}

	public String getPrefix() {
		String fn = (field != null) ? field.getJavaFieldName() : "";
		return clazz.getClazz().getName() + "." + fn;
	}
}