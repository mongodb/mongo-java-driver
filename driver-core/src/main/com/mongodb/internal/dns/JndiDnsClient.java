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

package com.mongodb.internal.dns;

import com.mongodb.MongoClientException;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.DnsException;
import com.mongodb.spi.dns.DnsWithResponseCodeException;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

public class JndiDnsClient implements DnsClient {
    private final InitialDirContext dirContext;

    public JndiDnsClient() {
        dirContext = createDnsDirContext();
    }

    @Override
    public List<String> getAttributeValues(final String name, final String type) throws DnsException {
        try {
            Attribute attribute = dirContext.getAttributes(name, new String[]{type}).get(type);
            if (attribute == null) {
                return null;
            }
            List<String> attributeValues = new ArrayList<>();
            NamingEnumeration<?> namingEnumeration = attribute.getAll();
            while (namingEnumeration.hasMore()) {
                attributeValues.add((String) namingEnumeration.next());
            }
            return attributeValues;
        } catch (NameNotFoundException e) {
            throw new DnsWithResponseCodeException(e.getMessage(), 3, e);
        } catch (NamingException e) {
            throw new DnsException(e.getMessage(), e);
        } finally {
            try {
                dirContext.close();
            } catch (NamingException e) {
                // ignore
            }
        }
    }

    /*
      It's unfortunate that we take a runtime dependency on com.sun.jndi.dns.DnsContextFactory.
      This is not guaranteed to work on all JVMs but in practice is expected to work on most.
    */
    private static InitialDirContext createDnsDirContext() {
        Hashtable<String, String> envProps = new Hashtable<String, String>();
        envProps.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");

        try {
            return new InitialDirContext(envProps);
        } catch (NamingException e) {
            // Just in case the provider url default has been changed to a non-dns pseudo url, fallback to the JDK default
            envProps.put(Context.PROVIDER_URL, "dns:");
            try {
                return new InitialDirContext(envProps);
            } catch (NamingException ex) {
                throw new MongoClientException("Unable to support mongodb+srv// style connections as the 'com.sun.jndi.dns.DnsContextFactory' "
                        + "class is not available in this JRE. A JNDI context is required for resolving SRV records.", e);
            }
        }
    }
}
