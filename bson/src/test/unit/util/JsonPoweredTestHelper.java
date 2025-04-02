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

package util;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.assertions.Assertions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public final class JsonPoweredTestHelper {

    private static final String SPECIFICATIONS_PREFIX = "/specifications/source/";

    public static BsonDocument getTestDocument(final String resourcePath) {
        BsonDocument testDocument = getTestDocumentWithMetaData(SPECIFICATIONS_PREFIX + resourcePath);
        testDocument.remove("resourcePath");
        testDocument.remove("fileName");
        return testDocument;
    }

    public static Collection<Object[]> getTestData(final String resourcePath) {
        List<Object[]> data = new ArrayList<>();
        for (BsonDocument document : getSpecTestDocuments(resourcePath)) {
            for (BsonValue test : document.getArray("tests")) {
                BsonDocument testDocument = test.asDocument();
                data.add(new Object[]{document.getString("fileName").getValue(),
                        testDocument.getString("description").getValue(),
                        testDocument.getString("uri", new BsonString("")).getValue(),
                        testDocument});
            }
        }
        return data;
    }

    public static List<BsonDocument> getSpecTestDocuments(final String resourcePath) {
        return getTestDocuments(SPECIFICATIONS_PREFIX + resourcePath);
    }

    public static List<BsonDocument> getTestDocuments(final String resourcePath) {
        List<BsonDocument> files = new ArrayList<>();
        try {
            URL urlResource = JsonPoweredTestHelper.class.getResource(resourcePath);
            if (urlResource == null) {
                Assertions.fail("No such resource: " + resourcePath);
            }

            URI resource = urlResource.toURI();
            try (FileSystem fileSystem = (resource.getScheme().equals("jar") ? FileSystems.newFileSystem(resource, Collections.emptyMap()) : null)) {
                Path myPath = Paths.get(resource);
                Files.walkFileTree(myPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(final Path filePath, final BasicFileAttributes attrs) throws IOException {
                        if (filePath.toString().endsWith(".json")) {
                            if (fileSystem == null) {
                                files.add(getTestDocumentWithMetaData(filePath.toString().substring(filePath.toString().lastIndexOf(resourcePath))));
                            } else {
                                files.add(getTestDocumentWithMetaData(filePath.toString()));
                            }
                        }
                        return super.visitFile(filePath, attrs);
                    }
                });
            }
        } catch (Exception e) {
            Assertions.fail("Unable to load resource: " + resourcePath, e);
        }

        if (files.isEmpty()) {
            Assertions.fail("No resources found in: " + resourcePath);
        }
        return files;
    }

    private static BsonDocument getTestDocumentWithMetaData(final String resourcePath) {
        BsonDocument testDocument = BsonDocument.parse(resourcePathToString(resourcePath));
        testDocument.append("resourcePath", new BsonString(resourcePath))
                .append("fileName", new BsonString(resourcePath.substring(resourcePath.lastIndexOf('/') + 1)));
        return testDocument;
    }

    private static String resourcePathToString(final String resourcePath)  {
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        String ls = System.lineSeparator();
        try (InputStream inputStream = JsonPoweredTestHelper.class.getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                Assertions.fail("Unable to load resource: " + resourcePath);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
                while ((line = reader.readLine()) != null) {
                    stringBuilder.append(line);
                    stringBuilder.append(ls);
                }
            }
        } catch (Exception e) {
            Assertions.fail("Unable to load resource", e);
        }
        return stringBuilder.toString();
    }

    private JsonPoweredTestHelper() {
    }
}
