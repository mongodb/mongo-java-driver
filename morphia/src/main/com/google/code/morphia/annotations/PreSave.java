package com.google.code.morphia.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Called right before DBCollection.save() is called. Changes made to the entity (java class) will not be persisted. This event will be called on top level Entities.
 * 
 * @author Scott Hernandez
 */
@Documented @Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface PreSave {
}
