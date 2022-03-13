package com.mongodb.connection.dns;

import java.util.function.Supplier;

/**
 * Factory for {@link DnsResolver} instances
 */
public final class DnsResolverFactory {

    private static Supplier<DnsResolver> dnsResolverSupplier;

    private DnsResolverFactory() {
        throw new IllegalAccessError("Utility class");
    }

    public static DnsResolver dnsResolver() {
        return dnsResolverSupplier == null ? DefaultDnsResolver.INSTANCE : dnsResolverSupplier.get();
    }

    public static void setDnsResolverSupplier(final Supplier<DnsResolver> dnsResolverSupplier) {
        DnsResolverFactory.dnsResolverSupplier = dnsResolverSupplier;
    }
}
