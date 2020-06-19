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
import com.mongodb.MongoConfigurationException;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Utility class for resolving SRV and TXT records.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class DefaultDnsResolver implements DnsResolver {

    /*
      The format of SRV record is
        priority weight port target.
      e.g.
        0 5 5060 example.com.

      The priority and weight are ignored, and we just concatenate the host (after removing the ending '.') and port with a
      ':' in between, as expected by ServerAddress.

      It's required that the srvHost has at least three parts (e.g. foo.bar.baz) and that all of the resolved hosts have a parent
      domain equal to the domain of the srvHost.
    */
    @Override
    public List<String> resolveHostFromSrvRecords(final String srvHost) {
        String srvHostDomain = srvHost.substring(srvHost.indexOf('.') + 1);
        List<String> srvHostDomainParts = asList(srvHostDomain.split("\\."));
        List<String> hosts = new ArrayList<String>();
        InitialDirContext dirContext = createDnsDirContext();
        try {
            Attributes attributes = dirContext.getAttributes("_mongodb._tcp." + srvHost, new String[]{"SRV"});
            Attribute attribute = attributes.get("SRV");
            if (attribute == null) {
                throw new MongoConfigurationException("No SRV records available for host " + srvHost);
            }
            NamingEnumeration<?> srvRecordEnumeration = attribute.getAll();
            while (srvRecordEnumeration.hasMore()) {
                String srvRecord = (String) srvRecordEnumeration.next();
                String[] split = srvRecord.split(" ");
                String resolvedHost = split[3].endsWith(".") ? split[3].substring(0, split[3].length() - 1) : split[3];
                String resolvedHostDomain = resolvedHost.substring(resolvedHost.indexOf('.') + 1);
                if (!sameParentDomain(srvHostDomainParts, resolvedHostDomain)) {
                    throw new MongoConfigurationException(
                            format("The SRV host name '%s'resolved to a host '%s 'that is not in a sub-domain of the SRV host.",
                                    srvHost, resolvedHost));
                }
                hosts.add(resolvedHost + ":" + split[2]);
            }

            if (hosts.isEmpty()) {
                throw new MongoConfigurationException("Unable to find any SRV records for host " + srvHost);
            }
        } catch (NamingException e) {
            throw new MongoConfigurationException("Unable to look up SRV record for host " + srvHost, e);
        } finally {
            try {
                dirContext.close();
            } catch (NamingException e) {
                // ignore
            }
        }
        return hosts;
    }

    private static boolean sameParentDomain(final List<String> srvHostDomainParts, final String resolvedHostDomain) {
        List<String> resolvedHostDomainParts = asList(resolvedHostDomain.split("\\."));
        if (srvHostDomainParts.size() > resolvedHostDomainParts.size()) {
            return false;
        }
        return resolvedHostDomainParts.subList(resolvedHostDomainParts.size() - srvHostDomainParts.size(), resolvedHostDomainParts.size())
                .equals(srvHostDomainParts);
    }

    /*
      A TXT record is just a string
      We require each to be one or more query parameters for a MongoDB connection string.
      Here we concatenate TXT records together with a '&' separator as required by connection strings
    */
    @Override
    public String resolveAdditionalQueryParametersFromTxtRecords(final String host) {
        String additionalQueryParameters = "";
        InitialDirContext dirContext = createDnsDirContext();
        try {
            Attributes attributes = dirContext.getAttributes(host, new String[]{"TXT"});
            Attribute attribute = attributes.get("TXT");
            if (attribute != null) {
                NamingEnumeration<?> txtRecordEnumeration = attribute.getAll();
                if (txtRecordEnumeration.hasMore()) {
                    // Remove all space characters, as the DNS resolver for TXT records inserts a space character
                    // between each character-string in a single TXT record.  That whitespace is spurious in
                    // this context and must be removed
                    additionalQueryParameters = ((String) txtRecordEnumeration.next()).replaceAll("\\s", "");

                    if (txtRecordEnumeration.hasMore()) {
                        throw new MongoConfigurationException(format("Multiple TXT records found for host '%s'.  Only one is permitted",
                                host));
                    }
                }
            }
        } catch (NamingException e) {
            throw new MongoConfigurationException("Unable to look up TXT record for host " + host, e);
        } finally {
            try {
                dirContext.close();
            } catch (NamingException e) {
                // ignore
            }
        }
        return additionalQueryParameters;
    }

    /*
      It's unfortunate that we take a runtime dependency on com.sun.jndi.dns.DnsContextFactory.
      This is not guaranteed to work on all JVMs but in practice is expected to work on most.
    */
    private static InitialDirContext createDnsDirContext() {
        Hashtable<String, String> envProps = new Hashtable<String, String>();
        envProps.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        envProps.put(Context.PROVIDER_URL, "dns:");
        try {
            return new InitialDirContext(envProps);
        } catch (NamingException e) {
            throw new MongoClientException("Unable to support mongodb+srv// style connections as the 'com.sun.jndi.dns.DnsContextFactory' "
                    + "class is not available in this JRE. A JNDI context is required for resolving SRV records.", e);
        }
    }
}
