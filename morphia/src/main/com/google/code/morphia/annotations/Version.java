/**
 * 
 */
package com.google.code.morphia.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.code.morphia.mapping.Mapper;

/**
 * supposed to be used on a Long or long field for optimistic locking.
 * 
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Version {
	String value() default Mapper.IGNORED_FIELDNAME;
}
