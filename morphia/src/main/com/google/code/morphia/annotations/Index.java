/*
 *  Copyright 2010 gauti.
 * 
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  under the License.
 */

package com.google.code.morphia.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines an index
 * @author Scott Hernandez
 */
@Documented @Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Index {
	/** List of fields (prepended with "-" for desc; defaults to asc) */
	String value();
	/** The name of the index to create; default is to let the mongodb create a name (in the form of key1_1/-1_key2_1/-1...*/
	String name() default "";
	/** Creates the index as a unique value index; inserting duplicates values in this field will cause errors */
	boolean unique() default false;
	/** Tells the unique index to drop duplicates silently when creating; only the first will be kept*/
	boolean dropDups() default false;
	/** Create the index in the background*/
	boolean background() default false;
	/** Create the index with the sparse option*/
	boolean sparse() default false;
	/** disables validation for the field name*/
	boolean disableValidation() default false;

}
