package com.google.code.morphia.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotation which helps migrate schemas by loading one of several possible properties
 * in the document into fields or methods.  This is typically used when a field is renamed,
 * allowing the field to be populated by both its current name and any prior names.</p>
 * 
 * <ul>
 * <li>When placed on a field, the additional names (document field) will be checked
 * when this field is loaded.  If the document contains data for more than one of the names,
 * an exception will be thrown. 
 * <li>When placed on a parameter to a method that takes a single parameter, the method
 * will be called with the data value.  As with fields, any ambiguity in the data (multiple
 * properties that would cause the method to be called) will produce an exception.  However,
 * {@code @AlsoLoad} on a method parameter *can* be used to override field names and "steal" the
 * value that would otherwise have been set on a field.  This can be useful when changing the
 * type of a field.</li>
 * </ul>
 * 
 * (orig @author Jeff Schnitzer <jeff@infohazard.org> for Objectify)
 * @author Scott Hernandez
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface AlsoLoad
{
	String[] value();
}