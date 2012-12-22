package com.google.code.morphia.mapping.validation.classrules;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Transient;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.validation.ClassConstraint;
import com.google.code.morphia.mapping.validation.ConstraintViolation;
import com.google.code.morphia.mapping.validation.ConstraintViolation.Level;
import com.google.code.morphia.utils.ReflectionUtils;

/**
 * @author josephpachod
 */
public class ContainsEmbeddedWithId implements ClassConstraint
{

    private boolean hasTypeFieldAnnotation(final Class<?> type, final Class<Id> class1)
    {
        for (Field field : ReflectionUtils.getDeclaredAndInheritedFields(type, true))
        {
            if (field.getAnnotation(class1) != null)
            {
                return true;
            }
        }
        return false;
    }

	public void check(final MappedClass mc, final Set<ConstraintViolation> ve)
    {
        Set<Class<?>> classesToInspect = new HashSet<Class<?>>();
        for (Field field : ReflectionUtils.getDeclaredAndInheritedFields(mc.getClazz(), true))
        {
            if (isFieldToInspect(field) && !field.isAnnotationPresent(Id.class))
            {
                classesToInspect.add(field.getType());
            }
        }
        checkRecursivelyHasNoIdAnnotationPresent(classesToInspect, new HashSet<Class<?>>(), mc, ve);
    }

    private boolean isFieldToInspect(final Field field)
    {
        return (!field.isAnnotationPresent(Transient.class) && !field.isAnnotationPresent(Reference.class));
    }

    private void checkRecursivelyHasNoIdAnnotationPresent(final Set<Class<?>> classesToInspect,
            final HashSet<Class<?>> alreadyInspectedClasses, final MappedClass mc, final Set<ConstraintViolation> ve)
    {
        for (Class<?> clazz : classesToInspect)
        {
            if (alreadyInspectedClasses.contains(clazz))
            {
                continue;
            }
            if (hasTypeFieldAnnotation(clazz, Id.class))
            {
                ve.add(new ConstraintViolation(Level.FATAL, mc, this.getClass(),
                        "You cannot use @Id on any field of an Embedded/Property object"));
            }
            alreadyInspectedClasses.add(clazz);
            Set<Class<?>> extraClassesToInspect = new HashSet<Class<?>>();
            for (Field field : ReflectionUtils.getDeclaredAndInheritedFields(clazz, true))
            {
                if (isFieldToInspect(field))
                {
                    extraClassesToInspect.add(field.getType());
                }
            }
            checkRecursivelyHasNoIdAnnotationPresent(extraClassesToInspect, alreadyInspectedClasses, mc, ve);
        }
    }
}
