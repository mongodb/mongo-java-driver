/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping.lazy;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public final class LazyFeatureDependencies {

    private static final Logr logger = MorphiaLoggerFactory.get(LazyFeatureDependencies.class);
    private static Boolean fullFilled;

    private LazyFeatureDependencies() {
    }

    public static boolean assertDependencyFullFilled() {
        final boolean fullfilled = testDependencyFullFilled();
        if (!fullfilled) {
            logger.warning("Lazy loading impossible due to missing dependencies.");
        }
        return fullfilled;
    }

    public static boolean testDependencyFullFilled() {
        if (fullFilled != null) {
            return fullFilled;
        }
        try {
            fullFilled = Class.forName("net.sf.cglib.proxy.Enhancer") != null
                         && Class.forName("com.thoughtworks.proxy.toys.hotswap.HotSwapping") != null;
        } catch (ClassNotFoundException e) {
            fullFilled = false;
        }
        return fullFilled;
    }

    /**
     * @return
     */
    public static LazyProxyFactory createDefaultProxyFactory() {
        if (testDependencyFullFilled()) {
            final String factoryClassName = "com.google.code.morphia.mapping.lazy.CGLibLazyProxyFactory";
            try {
                return (LazyProxyFactory) Class.forName(factoryClassName).newInstance();
            } catch (Exception e) {
                logger.error("While instanciating " + factoryClassName, e);
            }
        }
        return null;
    }
}
