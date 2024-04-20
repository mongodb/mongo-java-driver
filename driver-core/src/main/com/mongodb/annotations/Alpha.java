package com.mongodb.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies that a public API (public class, method or field) is in the early stages
 * of development and may undergo significant changes or even be removed in future releases.
 * An API bearing this annotation is exempt from any compatibility guarantees made by its
 * containing library.
 *
 * <p>It is discouraged for <i>applications</i> to use Alpha APIs in production environments due to their
 * possible instability and frequent changes, and potential performance implications. Similarly, it is inadvisable
 * for <i>libraries</i> (which get included on users' CLASSPATHs, outside the library developers' control)
 * to depend on Alpha APIs.
 *
 * <p> Alpha APIs should be used solely for experimental purposes and with the understanding that substantial
 * adjustments may be necessary during upgrades.
 *
 * <p>To report issues on this API, please visit the MongoDB JIRA issue tracker at
 * <a href="https://jira.mongodb.org/browse/JAVA">https://jira.mongodb.org/browse/JAVA</a>.</p>
 **/
@Retention(RetentionPolicy.CLASS)
@Target({
        ElementType.ANNOTATION_TYPE,
        ElementType.CONSTRUCTOR,
        ElementType.FIELD,
        ElementType.METHOD,
        ElementType.PACKAGE,
        ElementType.TYPE })
@Documented
@Alpha(Alpha.Reason.CLIENT)
public @interface Alpha {
    /**
     * @return The reason an API element is marked with {@link Alpha}.
     */
    Alpha.Reason[] value();

    /**
     * @see Alpha#value()
     */
    enum Reason {
        /**
         * Indicates that the driver API is either experimental or in development.
         * Use in production environments is strongly discouraged due to potential changes and instability.
         */
        CLIENT,
    }
}
