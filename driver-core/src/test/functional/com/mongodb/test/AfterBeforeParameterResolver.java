/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.test;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter;
import org.junit.jupiter.engine.extension.ExtensionRegistry;
import org.junit.platform.commons.util.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * The {@code AfterBeforeParameterResolver} supports passing parameterized test values to the {@code BeforeEach} and/or
 * the {@code AfterEach} methods.
 *
 * <p>Example usage:
 * <p>
 * {@code
 *   @ExtendWith(AfterBeforeParameterResolver.class)
 *   class AfterBeforeParameterResolverTest {
 *
 *     private TestEnum capturedParameter;
 *     @BeforeEach
 *     public void setup(TestEnum parameter) {
 *       capturedParameter = parameter;
 *     }
 *
 *     @ParameterizedTest
 *     @EnumSource(TestEnum.class)
 *     public void test(TestEnum parameter) {
 *       assertThat(parameter).isEqualTo(capturedParameter);
 *     }
 *
 *     enum TestEnum {
 *       PARAMETER_1,
 *       PARAMETER_2;
 *     }
 *   }
 * }
 *
 * @see <a href="https://code-case.hashnode.dev/how-to-pass-parameterized-test-parameters-to-beforeeachaftereach-method-in-junit5">AfterBeforeParameterResolver</a>
 * @see <a href="https://github.com/junit-team/junit5/issues/944">junit-team/junit5#944</a>
 */
public class AfterBeforeParameterResolver implements BeforeEachMethodAdapter, ParameterResolver {
    private ParameterResolver parameterisedTestParameterResolver = null;

    @Override
    public void invokeBeforeEachMethod(final ExtensionContext context, final ExtensionRegistry registry) {
        Optional<ParameterResolver> resolverOptional = registry.getExtensions(ParameterResolver.class)
                .stream()
                .filter(parameterResolver -> parameterResolver.getClass().getName().contains("ParameterizedTestParameterResolver"))
                .findFirst();
        if (!resolverOptional.isPresent()) {
            throw new IllegalStateException("ParameterizedTestParameterResolver missed in the registry. "
                    + "Probably it's not a Parameterized Test");
        } else {
            parameterisedTestParameterResolver = resolverOptional.get();
        }
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        if (isExecutedOnAfterOrBeforeMethod(parameterContext)) {
            ParameterContext pContext = getMappedContext(parameterContext, extensionContext);
            return parameterisedTestParameterResolver.supportsParameter(pContext, extensionContext);
        }
        return false;
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext, final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterisedTestParameterResolver.resolveParameter(getMappedContext(parameterContext, extensionContext), extensionContext);
    }

    private MappedParameterContext getMappedContext(final ParameterContext parameterContext, final ExtensionContext extensionContext) {
        return new MappedParameterContext(
                parameterContext.getIndex(),
                extensionContext.getRequiredTestMethod().getParameters()[parameterContext.getIndex()],
                parameterContext.getTarget());
    }


    private boolean isExecutedOnAfterOrBeforeMethod(final ParameterContext parameterContext) {
        return Arrays.stream(parameterContext.getDeclaringExecutable().getDeclaredAnnotations())
                .anyMatch(this::isAfterEachOrBeforeEachAnnotation);
    }

    private boolean isAfterEachOrBeforeEachAnnotation(final Annotation annotation) {
        return annotation.annotationType() == BeforeEach.class || annotation.annotationType() == AfterEach.class;
    }


    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static class MappedParameterContext implements ParameterContext {
        private final int index;
        private final Parameter parameter;
        private final Optional<Object> target;
        public MappedParameterContext(final int index, final Parameter parameter, final Optional<Object> target) {
            this.index = index;
            this.parameter = parameter;
            this.target = target;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public Parameter getParameter() {
            return parameter;
        }

        @Override
        public Optional<Object> getTarget() {
            return target;
        }

        @Override
        public boolean isAnnotated(final Class<? extends Annotation> annotationType) {
            return AnnotationUtils.isAnnotated(parameter, annotationType);
        }

        @Override
        public <A extends Annotation> Optional<A> findAnnotation(final Class<A> annotationType) {
            return AnnotationUtils.findAnnotation(parameter, annotationType);
        }

        @Override
        public <A extends Annotation> List<A> findRepeatableAnnotations(final Class<A> annotationType) {
            return AnnotationUtils.findRepeatableAnnotations(parameter, annotationType);
        }
    }
}
