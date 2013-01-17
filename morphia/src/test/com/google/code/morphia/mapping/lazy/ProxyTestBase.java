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

import com.google.code.morphia.TestBase;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedReference;
import org.junit.Assert;
import org.junit.Ignore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@Ignore
@SuppressWarnings("unchecked")
public class ProxyTestBase extends TestBase {

    protected void assertFetched(final Object e) {
        Assert.assertTrue(isFetched(e));
    }

    protected void assertNotFetched(final Object e) {
        Assert.assertFalse(isFetched(e));
    }

    protected boolean isFetched(final Object e) {
        return asProxiedReference(e).__isFetched();
    }

    protected ProxiedReference asProxiedReference(final Object e) {
        return (ProxiedReference) e;
    }

    protected void assertIsProxy(final Object p) {
        Assert.assertTrue(p instanceof ProxiedReference);
    }

    protected <T> T deserialize(final Object t) {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream os = new ObjectOutputStream(baos);
            os.writeObject(t);
            os.close();
            final byte[] ba = baos.toByteArray();

            return (T) new ObjectInputStream(new ByteArrayInputStream(ba))
                    .readObject();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }


}
