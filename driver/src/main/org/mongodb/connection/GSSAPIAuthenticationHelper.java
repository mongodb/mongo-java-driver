/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.mongodb.MongoCredential;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.util.HashMap;
import java.util.Map;

final class GSSAPIAuthenticationHelper {

    public static final String GSSAPI_MECHANISM_NAME = "GSSAPI";
    public static final String GSSAPI_OID = "1.2.840.113554.1.2.2";
    public static final String GSSAPI_MECHANISM = MongoCredential.GSSAPI_MECHANISM;

    public static SaslClient createSaslClient(final MongoCredential credential, final String host) {
        try {
            Map<String, Object> props = new HashMap<String, Object>();
            props.put(Sasl.CREDENTIALS, getGSSCredential(credential.getUserName()));

            return Sasl.createSaslClient(new String[]{GSSAPI_MECHANISM}, credential.getUserName(), SaslAuthenticator.MONGODB_PROTOCOL,
                    host, props, null);
        } catch (SaslException e) {
            throw new MongoSecurityException(credential, "Exception initializing SASL client", e);
        } catch (GSSException e) {
            throw new MongoSecurityException(credential, "Exception initializing GSSAPI credentials", e);
        }
    }

    private static GSSCredential getGSSCredential(final String userName) throws GSSException {
        Oid krb5Mechanism = new Oid(GSSAPI_OID);
        GSSManager manager = GSSManager.getInstance();
        GSSName name = manager.createName(userName, GSSName.NT_USER_NAME);
        return manager.createCredential(name, GSSCredential.INDEFINITE_LIFETIME,
                krb5Mechanism, GSSCredential.INITIATE_ONLY);
    }

    private GSSAPIAuthenticationHelper() {
    }
}
