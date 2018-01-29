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

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;

import javax.net.ssl.SSLParameters;
import java.lang.reflect.InvocationTargetException;

/**
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class SslHelper {

    // this will end up as null if running on a release prior to Java 8, in which case SNI will be silently disabled
    private static final SniSslHelper SNI_SSL_HELPER;

    static {
        SniSslHelper sniSslHelper;
        try {
            sniSslHelper = (SniSslHelper) Class.forName("com.mongodb.internal.connection.Java8SniSslHelper")
                                                  .getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            // this is unexpected as it means the Java8SniSslHelper class itself is not found
            throw new ExceptionInInitializerError(e);
        } catch (InstantiationException e) {
            // this is unexpected as it means Java8SniSslHelper can't be instantiated
            throw new ExceptionInInitializerError(e);
        } catch (IllegalAccessException e) {
            // this is unexpected as it means Java8SniSslHelper's constructor isn't accessible
            throw new ExceptionInInitializerError(e);
        } catch (NoSuchMethodException e) {
            // this is unexpected as it means Java8SniSslHelper has no no-args constructor
            throw new ExceptionInInitializerError(e);
        } catch (InvocationTargetException e) {
            // this is unexpected as it means Java8SniSslHelper's constructor threw an exception
            throw new ExceptionInInitializerError(e.getTargetException());
        } catch (LinkageError t) {
            // this is expected if running on a release prior to Java 8.  We want to just fail silently here
            sniSslHelper = null;
        }

        SNI_SSL_HELPER = sniSslHelper;
    }

    /**
     * Enable HTTP endpoint verification on the given SSL parameters.
     *
     * @param sslParameters The original SSL parameters
     */
    public static void enableHostNameVerification(final SSLParameters sslParameters) {
        sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    }

    /**
     * Enable SNI if running on Java 8 or later.  Otherwise fail silently to enable SNI.
     *
     * @param address       the server address
     * @param sslParameters the SSL parameters
     */
    public static void enableSni(final ServerAddress address, final SSLParameters sslParameters) {
        if (SNI_SSL_HELPER != null) {
            SNI_SSL_HELPER.enableSni(address, sslParameters);
        }
    }

    private SslHelper() {
    }
}
