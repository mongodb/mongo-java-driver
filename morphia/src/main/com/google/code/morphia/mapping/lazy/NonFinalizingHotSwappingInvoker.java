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

package com.google.code.morphia.mapping.lazy;

import com.thoughtworks.proxy.ProxyFactory;
import com.thoughtworks.proxy.kit.ObjectReference;
import com.thoughtworks.proxy.toys.delegate.DelegationMode;
import com.thoughtworks.proxy.toys.hotswap.HotSwappingInvoker;

import java.lang.reflect.Method;

@SuppressWarnings({ "rawtypes", "unchecked" })
class NonFinalizingHotSwappingInvoker extends HotSwappingInvoker {

    public NonFinalizingHotSwappingInvoker(final Class[] types, final ProxyFactory proxyFactory,
                                           final ObjectReference delegateReference,
                                           final DelegationMode delegationMode) {
        super(types, proxyFactory, delegateReference, delegationMode);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if ("finalize".equals(method.getName()) && args != null && args.length == 0) {
            return null;
        }

        return super.invoke(proxy, method, args);
    }

}
