package com.google.code.morphia.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>When placed on an entity field, the field will not be written to mongodb.
 * It will, however, be loaded normally.  This is particularly useful in concert with
 * {@code @PostLoad} and {@code @PrePersist} to transform your data.</p>
 **/

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.TYPE})
public @interface NotSaved {
	
}
