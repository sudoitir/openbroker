package io.github.sudoitir.openbroker;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an API whose shape a planned change will redesign. It works, and drivers implement it, but it
 * is exempt from the compatibility check: it may change in any release until the mark is removed.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.PACKAGE})
public @interface Incubating {}
