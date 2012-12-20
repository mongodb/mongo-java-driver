package com.google.code.morphia.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
/**
 * Indicates that this field can be constructed from the stored fields; it doesn't require a no-args constructor. 
 * Please list the names of args/fields, in order.
 * @author Scott Hernandez
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConstructorArgs {
	String[] value();
}
