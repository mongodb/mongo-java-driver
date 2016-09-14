/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.client.MongoDriverInformation;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static java.lang.String.format;
import static java.lang.System.getProperty;

final class ClientMetadataHelper {
    public static final BsonDocument CLIENT_METADATA_DOCUMENT = new BsonDocument();

    private static final String SEPARATOR = "|";

    private static final String APPLICATION_FIELD = "application";
    private static final String APPLICATION_NAME_FIELD = "name";

    private static final String DRIVER_FIELD = "driver";
    private static final String DRIVER_NAME_FIELD = "name";
    private static final String DRIVER_VERSION_FIELD = "version";

    private static final String PLATFORM_FIELD = "platform";

    private static final String OS_FIELD = "os";
    private static final String OS_TYPE_FIELD = "type";
    private static final String OS_NAME_FIELD = "name";
    private static final String OS_ARCHITECTURE_FIELD = "architecture";
    private static final String OS_VERSION_FIELD = "version";

    private static final int MAXIMUM_CLIENT_METADATA_ENCODED_SIZE = 512;

    static {
        BsonDocument driverMetadataDocument = addDriverInformation(null, new BsonDocument());
        CLIENT_METADATA_DOCUMENT.append(DRIVER_FIELD, driverMetadataDocument.get(DRIVER_FIELD));

        try {
            String operatingSystemName = getProperty("os.name", "unknown");
            CLIENT_METADATA_DOCUMENT.append(OS_FIELD, new BsonDocument()
                                                              .append(OS_TYPE_FIELD,
                                                                      new BsonString(getOperatingSystemType(operatingSystemName)))
                                                              .append(OS_NAME_FIELD,
                                                                      new BsonString(operatingSystemName))
                                                              .append(OS_ARCHITECTURE_FIELD,
                                                                      new BsonString(getProperty("os.arch", "unknown")))
                                                              .append(OS_VERSION_FIELD,
                                                                      new BsonString(getProperty("os.version", "unknown"))))
                    .append(PLATFORM_FIELD, driverMetadataDocument.get(PLATFORM_FIELD, new BsonString("")));
        } catch (SecurityException e) {
            // do nothing
        }
    }

    private static String getOperatingSystemType(final String operatingSystemName) {
        if (nameMatches(operatingSystemName, "linux")) {
            return "Linux";
        } else if (nameMatches(operatingSystemName, "mac")) {
            return "Darwin";
        } else if (nameMatches(operatingSystemName, "windows")) {
            return  "Windows";
        } else if (nameMatches(operatingSystemName, "hp-ux", "aix", "irix", "solaris", "sunos")) {
            return "Unix";
        } else {
            return  "unknown";
        }
    }

    private static boolean nameMatches(final String name, final String... prefixes) {
        for (String prefix : prefixes) {
            if (name.toLowerCase().startsWith(prefix.toLowerCase())) {
                return true;
            }
        }
        return false;
    }

    private static String getDriverVersion() {
        String driverVersion = "unknown";

        try {
            String path = InternalStreamConnectionInitializer.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            URL jarUrl = path.endsWith(".jar") ? new URL("jar:file:" + path + "!/") : null;
            if (jarUrl != null) {
                JarURLConnection jarURLConnection = (JarURLConnection) jarUrl.openConnection();
                Manifest manifest = jarURLConnection.getManifest();
                String version = (String) manifest.getMainAttributes().get(new Attributes.Name("Build-Version"));
                if (version != null) {
                    driverVersion = version;
                }
            }
        } catch (Exception e) {
            // do nothing
        }
        return driverVersion;
    }
    static BsonDocument createClientMetadataDocument(final String applicationName) {
        return createClientMetadataDocument(applicationName, null);
    }

    static BsonDocument createClientMetadataDocument(final String applicationName, final MongoDriverInformation mongoDriverInformation) {
        return createClientMetadataDocument(applicationName, mongoDriverInformation, CLIENT_METADATA_DOCUMENT);
    }

    static BsonDocument createClientMetadataDocument(final String applicationName, final MongoDriverInformation mongoDriverInformation,
                                                     final BsonDocument templateDocument) {
        if (applicationName != null) {
            isTrueArgument("applicationName UTF-8 encoding length <= 128",
                    applicationName.getBytes(Charset.forName("UTF-8")).length <= 128);
        }

        BsonDocument document = templateDocument.clone();
        if (applicationName != null) {
            document.append(APPLICATION_FIELD, new BsonDocument(APPLICATION_NAME_FIELD, new BsonString(applicationName)));
        }

        if (mongoDriverInformation != null) {
            addDriverInformation(mongoDriverInformation, document);
        }

        if (clientMetadataDocumentTooLarge(document)) {
            // first try: remove the three optional fields in the 'os' document, if it exists (may not if the security manager is configured
            // to disallow access to system properties)
            BsonDocument operatingSystemDocument = document.getDocument(OS_FIELD, null);
            if (operatingSystemDocument != null) {
                operatingSystemDocument.remove(OS_VERSION_FIELD);
                operatingSystemDocument.remove(OS_ARCHITECTURE_FIELD);
                operatingSystemDocument.remove(OS_NAME_FIELD);
            }
            if (operatingSystemDocument == null || clientMetadataDocumentTooLarge(document)) {
                // second try: remove the optional 'platform' field
                document.remove(PLATFORM_FIELD);
                if (clientMetadataDocumentTooLarge(document)) {
                    // Third try: Try the minimum required amount of data.
                    document = new BsonDocument(DRIVER_FIELD, templateDocument.getDocument(DRIVER_FIELD));
                    document.append(OS_FIELD, new BsonDocument(OS_TYPE_FIELD, new BsonString("unknown")));
                    if (clientMetadataDocumentTooLarge(document)) {
                        // Worst case scenario: give up and don't send any client metadata at all
                        document = null;
                    }
                }
            }
        }
        return document;
    }

    private static BsonDocument addDriverInformation(final MongoDriverInformation mongoDriverInformation, final BsonDocument document) {
        MongoDriverInformation driverInformation = getDriverInformation(mongoDriverInformation);
        BsonDocument driverMetadataDocument = new BsonDocument(DRIVER_NAME_FIELD, listToBsonString(driverInformation.getDriverNames()))
                .append(DRIVER_VERSION_FIELD, listToBsonString(driverInformation.getDriverVersions()));
        document.append(DRIVER_FIELD, driverMetadataDocument);
        document.append(PLATFORM_FIELD, listToBsonString(driverInformation.getDriverPlatforms()));
        return document;
    }

    static boolean clientMetadataDocumentTooLarge(final BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer(MAXIMUM_CLIENT_METADATA_ENCODED_SIZE);
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        return buffer.getPosition() > MAXIMUM_CLIENT_METADATA_ENCODED_SIZE;
    }

    static MongoDriverInformation getDriverInformation(final MongoDriverInformation mongoDriverInformation) {
        MongoDriverInformation.Builder builder = mongoDriverInformation != null ? MongoDriverInformation.builder(mongoDriverInformation)
                : MongoDriverInformation.builder();
        return builder
                .driverName("mongo-java-driver")
                .driverVersion(getDriverVersion())
                .driverPlatform(format("Java/%s/%s", getProperty("java.vendor", "unknown-vendor"),
                        getProperty("java.runtime.version", "unknown-version")))
                .build();
    }

    static BsonString listToBsonString(final List<String> listOfStrings) {
        StringBuilder stringBuilder = new StringBuilder();
        int i = 0;
        for (String val : listOfStrings) {
            if (i > 0) {
                stringBuilder.append(SEPARATOR);
            }
            stringBuilder.append(val);
            i++;
        }
        return new BsonString(stringBuilder.toString());
    }

    private ClientMetadataHelper() {
    }
}
