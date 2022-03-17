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

import com.mongodb.MongoConfigurationException;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.DnsClientProvider;
import com.mongodb.spi.dns.DnsException;
import com.mongodb.spi.dns.DnsWithResponseCodeException;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 * Utility class for resolving SRV and TXT records.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class DefaultDnsResolver implements DnsResolver {

    private final DnsClient dnsClient;

    public DefaultDnsResolver() {
        ServiceLoader<DnsClientProvider> loader = ServiceLoader.load(DnsClientProvider.class);
        DnsClient dnsClientFromServiceLoader = null;
        for (DnsClientProvider dnsClientProvider : loader) {
            dnsClientFromServiceLoader = dnsClientProvider.create();
            break;
        }

        if (dnsClientFromServiceLoader == null) {
            dnsClient = new JndiDnsClient();
        } else {
            dnsClient = dnsClientFromServiceLoader;
        }
    }

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
    public List<String> resolveHostFromSrvRecords(final String srvHost, final String srvServiceName) {
        String srvHostDomain = srvHost.substring(srvHost.indexOf('.') + 1);
        List<String> srvHostDomainParts = asList(srvHostDomain.split("\\."));
        List<String> hosts = new ArrayList<>();
        try {
            List<String> srvAttributeValues = dnsClient.getResourceRecordData("_" + srvServiceName + "._tcp." + srvHost, "SRV");
            if (srvAttributeValues == null || srvAttributeValues.isEmpty()) {
                throw new MongoConfigurationException("No SRV records available for " + "_mongodb._tcp." + srvHost);
            }

            for (String srvRecord : srvAttributeValues) {
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

        } catch (DnsException e) {
            throw new MongoConfigurationException("Unable to look up SRV record for host " + srvHost, e);
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
        try {
            List<String> attributeValues = dnsClient.getResourceRecordData(host, "TXT");
            if (attributeValues == null || attributeValues.isEmpty()) {
                return "";
            }
            if (attributeValues.size() > 1) {
                throw new MongoConfigurationException(format("Multiple TXT records found for host '%s'.  Only one is permitted",
                        host));
            }
            // Remove all space characters, as the DNS resolver for TXT records inserts a space character
            // between each character-string in a single TXT record.  That whitespace is spurious in
            // this context and must be removed
            return attributeValues.get(0).replaceAll("\\s", "");
        } catch (DnsWithResponseCodeException e) {
            // ignore NXDomain error (error code 3, "Non-Existent Domain)
            if (e.getResponseCode() != 3) {
                throw new MongoConfigurationException("Unable to look up TXT record for host " + host, e);
            }
            return "";
        } catch (DnsException e) {
            throw new MongoConfigurationException("Unable to look up TXT record for host " + host, e);
        }
    }
}
