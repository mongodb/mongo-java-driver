# QE Prefix/Suffix GA + rename Text→String — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote QE prefix/suffix text queries to GA and rename the public explicit-encryption API from "Text" to "String" (with deprecated aliases), riding on the libmongocrypt 1.19.1 bump.

**Architecture:** Add a new public `StringOptions` class mirroring `TextOptions`; deprecate `TextOptions`. On `EncryptOptions`, add `stringOptions(...)`/`getStringOptions()` and deprecate `textOptions(...)`/`getTextOptions()`, keeping two independent backing fields. `EncryptOptionsHelper` resolves `stringOptions` first, falling back to `textOptions` (mirrors Node's `stringOptions ?? textOptions`). The JNA layer is unchanged. The QE text explicit-encryption prose test is rewritten to spec test "27. String Explicit Encryption".

**Tech Stack:** Java 8 (driver-core, driver-sync), JNA (mongodb-crypt), JUnit 5, libmongocrypt 1.19.1.

## Global Constraints

- Java 8 language level — no `var`, records, text blocks, switch expressions, `Stream.toList()`, etc.
- Copyright header `Copyright 2008-present MongoDB, Inc.` on every new file.
- No `System.out`/`System.err`/`printStackTrace` — use SLF4J if logging is needed.
- Nullability annotations: `com.mongodb.lang.Nullable` / `NonNull`.
- New `@Alpha(Reason.SERVER)` API for `StringOptions` (matches current `TextOptions`).
- Binary compatibility: all changes additive or deprecated-retained — **no breaking change, no major bump**.
- libmongocrypt floor for the rewritten prose test: **1.19.1** (already on the branch).
- Valid pairings (verified): the `"String"` algorithm is required for query types `prefix`, `suffix`, `prefixPreview`, `suffixPreview`, `substringPreview`. The `"TextPreview"` algorithm is rejected by libmongocrypt 1.19.x.
- Do not modify the spec submodule (`testing/resources/specifications/`) — already bumped.
- Run `./gradlew spotlessApply` before committing; do not reformat code outside your change.

---

### Task 1: Add `StringOptions`, deprecate `TextOptions`

**Files:**
- Create: `driver-core/src/main/com/mongodb/client/model/vault/StringOptions.java`
- Modify: `driver-core/src/main/com/mongodb/client/model/vault/TextOptions.java`
- Test: `driver-core/src/test/unit/com/mongodb/client/model/vault/StringOptionsTest.java`

**Interfaces:**
- Produces: `public class StringOptions` with fluent setters `caseSensitive(boolean)`, `diacriticSensitive(boolean)`, `prefixOptions(@Nullable BsonDocument)`, `suffixOptions(@Nullable BsonDocument)`, `substringOptions(@Nullable BsonDocument)` (each returns `StringOptions`); getters `boolean getCaseSensitive()`, `boolean getDiacriticSensitive()`, `@Nullable BsonDocument getPrefixOptions()`, `getSuffixOptions()`, `getSubstringOptions()`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/client/model/vault/StringOptionsTest.java`:

```java
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

package com.mongodb.client.model.vault;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StringOptionsTest {

    @Test
    void shouldRoundTripAllProperties() {
        BsonDocument prefix = BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}");
        BsonDocument suffix = BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}");
        BsonDocument substring = BsonDocument.parse("{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}");

        StringOptions options = new StringOptions()
                .caseSensitive(true)
                .diacriticSensitive(true)
                .prefixOptions(prefix)
                .suffixOptions(suffix)
                .substringOptions(substring);

        assertTrue(options.getCaseSensitive());
        assertTrue(options.getDiacriticSensitive());
        assertEquals(prefix, options.getPrefixOptions());
        assertEquals(suffix, options.getSuffixOptions());
        assertEquals(substring, options.getSubstringOptions());
    }

    @Test
    void shouldDefaultOptionDocumentsToNull() {
        StringOptions options = new StringOptions();
        assertNull(options.getPrefixOptions());
        assertNull(options.getSuffixOptions());
        assertNull(options.getSubstringOptions());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.vault.StringOptionsTest"`
Expected: FAIL — compilation error, `StringOptions` does not exist.

- [ ] **Step 3: Create `StringOptions`**

Create `driver-core/src/main/com/mongodb/client/model/vault/StringOptions.java` by copying the current `TextOptions` body verbatim, renaming the class to `StringOptions`, updating the class Javadoc to say "String options for a Queryable Encryption field that supports string queries" and noting `substringOptions` is experimental/preview. Keep `@Alpha(Reason.SERVER)`, `@since 5.6`, `@mongodb.server.release 8.2`. The full file:

```java
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

package com.mongodb.client.model.vault;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

/**
 * String options for a Queryable Encryption field that supports string queries (prefix, suffix, and substring).
 *
 * <p>Note: StringOptions is in Alpha and subject to backwards breaking changes. The {@code substring} query support
 * is experimental (preview) and may change in a future non-major release.
 *
 * @since 5.6
 * @mongodb.server.release 8.2
 * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
 */
@Alpha(Reason.SERVER)
public class StringOptions {
    private Boolean caseSensitive;
    private Boolean diacriticSensitive;
    @Nullable
    private BsonDocument prefixOptions;
    @Nullable
    private BsonDocument suffixOptions;
    @Nullable
    private BsonDocument substringOptions;

    /**
     * Construct a new instance
     */
    public StringOptions() {
    }

    /**
     * @return true if string indexes for this field are case sensitive.
     */
    public boolean getCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Set case sensitivity
     *
     * @param caseSensitive true if string indexes are case sensitive
     * @return this
     */
    public StringOptions caseSensitive(final boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    /**
     * @return true if string indexes are diacritic sensitive
     */
    public boolean getDiacriticSensitive() {
        return diacriticSensitive;
    }

    /**
     * Set diacritic sensitivity
     *
     * @param diacriticSensitive true if string indexes are diacritic sensitive
     * @return this
     */
    public StringOptions diacriticSensitive(final boolean diacriticSensitive) {
        this.diacriticSensitive = diacriticSensitive;
        return this;
    }

    /**
     * Set the prefix options.
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param prefixOptions the prefix options or null
     * @return this
     */
    public StringOptions prefixOptions(@Nullable final BsonDocument prefixOptions) {
        this.prefixOptions = prefixOptions;
        return this;
    }

    /**
     * @see #prefixOptions(BsonDocument)
     * @return the prefix options document or null
     */
    @Nullable
    public BsonDocument getPrefixOptions() {
        return prefixOptions;
    }

    /**
     * Set the suffix options.
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param suffixOptions the suffix options or null
     * @return this
     */
    public StringOptions suffixOptions(@Nullable final BsonDocument suffixOptions) {
        this.suffixOptions = suffixOptions;
        return this;
    }

    /**
     * @see #suffixOptions(BsonDocument)
     * @return the suffix options document or null
     */
    @Nullable
    public BsonDocument getSuffixOptions() {
        return suffixOptions;
    }

    /**
     * Set the substring options.
     *
     * <p>Note: substring string queries are experimental (preview) and may change in a future non-major release.</p>
     *
     * <p>Expected to be a {@link BsonDocument} in the format of:</p>
     *
     * <pre>
     * {@code
     *   {
     *    // strMaxLength is the maximum allowed length to insert. Inserting longer strings will error.
     *    strMaxLength: BsonInt32,
     *    // strMinQueryLength is the minimum allowed query length. Querying with a shorter string will error.
     *    strMinQueryLength: BsonInt32,
     *    // strMaxQueryLength is the maximum allowed query length. Querying with a longer string will error.
     *    strMaxQueryLength: BsonInt32
     *   }
     * }
     * </pre>
     *
     * @param substringOptions the substring options or null
     * @return this
     */
    public StringOptions substringOptions(@Nullable final BsonDocument substringOptions) {
        this.substringOptions = substringOptions;
        return this;
    }

    /**
     * @see #substringOptions(BsonDocument)
     * @return the substring options document or null
     */
    @Nullable
    public BsonDocument getSubstringOptions() {
        return substringOptions;
    }
}
```

- [ ] **Step 4: Deprecate `TextOptions`**

In `driver-core/src/main/com/mongodb/client/model/vault/TextOptions.java`, leave the body unchanged but add a deprecation. Update the class Javadoc and annotation block (lines 24–34). Replace:

```java
/**
 * Text options for a Queryable Encryption field that supports text queries.
 *
 * <p>Note: TextOptions is in Alpha and subject to backwards breaking changes.
 *
 * @since 5.6
 * @mongodb.server.release 8.2
 * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
 */
@Alpha(Reason.SERVER)
public class TextOptions {
```

with:

```java
/**
 * Text options for a Queryable Encryption field that supports text queries.
 *
 * <p>Note: TextOptions is in Alpha and subject to backwards breaking changes.
 *
 * @since 5.6
 * @mongodb.server.release 8.2
 * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
 * @deprecated Use {@link StringOptions} instead.
 */
@Deprecated
@Alpha(Reason.SERVER)
public class TextOptions {
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.vault.StringOptionsTest"`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
./gradlew :driver-core:spotlessApply
git add driver-core/src/main/com/mongodb/client/model/vault/StringOptions.java \
        driver-core/src/main/com/mongodb/client/model/vault/TextOptions.java \
        driver-core/src/test/unit/com/mongodb/client/model/vault/StringOptionsTest.java
git commit -m "feat(JAVA-6168): add StringOptions and deprecate TextOptions"
```

---

### Task 2: `EncryptOptions` — add `stringOptions`, deprecate `textOptions`, update docs

**Files:**
- Modify: `driver-core/src/main/com/mongodb/client/model/vault/EncryptOptions.java`
- Test: `driver-core/src/test/unit/com/mongodb/client/model/vault/EncryptOptionsTest.java`

**Interfaces:**
- Consumes: `StringOptions`, `TextOptions` (Task 1).
- Produces: `EncryptOptions stringOptions(@Nullable StringOptions)`, `@Nullable StringOptions getStringOptions()`; deprecated `EncryptOptions textOptions(@Nullable TextOptions)`, `@Nullable TextOptions getTextOptions()`. Two independent backing fields `stringOptions` and `textOptions`.

- [ ] **Step 1: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/client/model/vault/EncryptOptionsTest.java`:

```java
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

package com.mongodb.client.model.vault;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class EncryptOptionsTest {

    @Test
    void shouldStoreStringOptions() {
        StringOptions stringOptions = new StringOptions().caseSensitive(true);
        EncryptOptions options = new EncryptOptions("String").stringOptions(stringOptions);
        assertSame(stringOptions, options.getStringOptions());
        assertNull(options.getTextOptions());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldStoreDeprecatedTextOptionsIndependently() {
        TextOptions textOptions = new TextOptions().caseSensitive(true);
        EncryptOptions options = new EncryptOptions("String").textOptions(textOptions);
        assertSame(textOptions, options.getTextOptions());
        assertNull(options.getStringOptions());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.vault.EncryptOptionsTest"`
Expected: FAIL — `stringOptions`/`getStringOptions` do not exist.

- [ ] **Step 3: Add the field**

In `EncryptOptions.java`, after line 36 (`private TextOptions textOptions;`) add:

```java
    private StringOptions stringOptions;
```

- [ ] **Step 4: Add `stringOptions(...)` and `getStringOptions()`**

In `EncryptOptions.java`, immediately after the existing `getTextOptions()` method (ends at line 233, before the `toString()` at line 235), insert:

```java
    /**
     * The StringOptions
     *
     * <p>It is an error to set StringOptions when the algorithm is not "String".
     * @param stringOptions the string options
     * @return this
     * @since 5.6
     * @mongodb.server.release 8.2
     * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
     */
    @Alpha(Reason.SERVER)
    public EncryptOptions stringOptions(@Nullable final StringOptions stringOptions) {
        this.stringOptions = stringOptions;
        return this;
    }

    /**
     * Gets the StringOptions
     * @see #stringOptions(StringOptions)
     * @return the string options or null if not set
     * @since 5.6
     * @mongodb.server.release 8.2
     * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
     */
    @Alpha(Reason.SERVER)
    @Nullable
    public StringOptions getStringOptions() {
        return stringOptions;
    }
```

- [ ] **Step 5: Deprecate `textOptions(...)` and `getTextOptions()`**

In `EncryptOptions.java`, update the existing `textOptions(...)` (lines ~205–219) and `getTextOptions()` (lines ~221–233) Javadocs to add `@deprecated Use {@link #stringOptions(StringOptions)} instead.` / `@deprecated Use {@link #getStringOptions()} instead.` and annotate both methods `@Deprecated`. The method bodies are unchanged. Example for the setter:

```java
    /**
     * The TextOptions
     *
     * <p>It is an error to set TextOptions when the algorithm is not "TextPreview".
     * @param textOptions the text options
     * @return this
     * @since 5.6
     * @mongodb.server.release 8.2
     * @mongodb.driver.manual /core/queryable-encryption/ queryable encryption
     * @deprecated Use {@link #stringOptions(StringOptions)} instead.
     */
    @Deprecated
    @Alpha(Reason.SERVER)
    public EncryptOptions textOptions(@Nullable final TextOptions textOptions) {
        this.textOptions = textOptions;
        return this;
    }
```

Apply the analogous `@Deprecated` + `@deprecated Use {@link #getStringOptions()} instead.` change to `getTextOptions()`.

- [ ] **Step 6: Update the algorithm and queryType Javadocs**

In `getAlgorithm()` (lines ~49–65), change the bullet `<li>TextPreview</li>` to `<li>String</li>` and replace the "TextPreview" preview note paragraph with:

```java
     * <p>The "String" algorithm supports Queryable Encryption prefix and suffix string queries, and (in preview)
     *   substring queries. Use the "String" algorithm with query types "prefix"/"suffix" (server 9.0+) or the
     *   deprecated aliases "prefixPreview"/"suffixPreview" (server 8.2 to pre-9.0), and "substringPreview"
     *   (experimental).</p>
```

In `queryType(...)` (lines ~149–162), replace the line:

```java
     * <p>Currently, we support only "equality", "range", "prefixPreview", "suffixPreview" or "substringPreview" queryType.</p>
     * <p>It is an error to set queryType when the algorithm is not "Indexed", "Range" or "TextPreview".</p>
```

with:

```java
     * <p>Currently, we support only "equality", "range", "prefix", "suffix", "prefixPreview", "suffixPreview" or
     * "substringPreview" queryType.</p>
     * <p>The "prefix", "suffix", "prefixPreview", "suffixPreview" and "substringPreview" query types are only valid
     * with the "String" algorithm. "prefixPreview"/"suffixPreview" are deprecated aliases supported for servers 8.2
     * to pre-9.0; use "prefix"/"suffix" on server 9.0+.</p>
     * <p>It is an error to set queryType when the algorithm is not "Indexed", "Range" or "String".</p>
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `./gradlew :driver-core:test --tests "com.mongodb.client.model.vault.EncryptOptionsTest"`
Expected: PASS (2 tests).

- [ ] **Step 8: Commit**

```bash
./gradlew :driver-core:spotlessApply
git add driver-core/src/main/com/mongodb/client/model/vault/EncryptOptions.java \
        driver-core/src/test/unit/com/mongodb/client/model/vault/EncryptOptionsTest.java
git commit -m "feat(JAVA-6168): add EncryptOptions.stringOptions and deprecate textOptions"
```

---

### Task 3: `EncryptOptionsHelper` — resolve stringOptions, fall back to textOptions

**Files:**
- Modify: `driver-core/src/main/com/mongodb/internal/client/vault/EncryptOptionsHelper.java`
- Test: `driver-core/src/test/unit/com/mongodb/internal/client/vault/EncryptOptionsHelperTest.java`

**Interfaces:**
- Consumes: `EncryptOptions.getStringOptions()`, `EncryptOptions.getTextOptions()` (Task 2); `MongoExplicitEncryptOptions.getTextOptions()` (unchanged internal type).
- Produces: unchanged public method `asMongoExplicitEncryptOptions(EncryptOptions)`.

- [ ] **Step 1: Write the failing test**

Create `driver-core/src/test/unit/com/mongodb/internal/client/vault/EncryptOptionsHelperTest.java`:

```java
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

package com.mongodb.internal.client.vault;

import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.StringOptions;
import com.mongodb.client.model.vault.TextOptions;
import com.mongodb.internal.crypt.capi.MongoExplicitEncryptOptions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EncryptOptionsHelperTest {

    @Test
    void shouldMapStringOptionsToTextOptionsDocument() {
        EncryptOptions options = new EncryptOptions("String").stringOptions(new StringOptions()
                .caseSensitive(true)
                .diacriticSensitive(false)
                .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));

        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(options);

        assertEquals(BsonDocument.parse("{caseSensitive: true, diacriticSensitive: false, "
                        + "prefix: {strMaxQueryLength: 10, strMinQueryLength: 2}}"),
                result.getTextOptions());
    }

    @Test
    @SuppressWarnings("deprecation")
    void shouldFallBackToDeprecatedTextOptions() {
        EncryptOptions options = new EncryptOptions("String").textOptions(new TextOptions()
                .caseSensitive(true)
                .diacriticSensitive(true)
                .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));

        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(options);

        assertEquals(BsonDocument.parse("{caseSensitive: true, diacriticSensitive: true, "
                        + "suffix: {strMaxQueryLength: 10, strMinQueryLength: 2}}"),
                result.getTextOptions());
    }

    @Test
    void shouldLeaveTextOptionsNullWhenNeitherSet() {
        MongoExplicitEncryptOptions result = EncryptOptionsHelper.asMongoExplicitEncryptOptions(
                new EncryptOptions("Indexed"));
        assertNull(result.getTextOptions());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.client.vault.EncryptOptionsHelperTest"`
Expected: FAIL — helper still reads only `getTextOptions()`, so `shouldMapStringOptionsToTextOptionsDocument` fails (text options null).

- [ ] **Step 3: Update the helper**

In `EncryptOptionsHelper.java`, add the import `import com.mongodb.client.model.vault.StringOptions;` (after the `RangeOptions` import). Then replace the `TextOptions` block (lines 76–97):

```java
        TextOptions textOptions = options.getTextOptions();
        if (textOptions != null) {
            BsonDocument textOptionsDocument = new BsonDocument();
            textOptionsDocument.put("caseSensitive", BsonBoolean.valueOf(textOptions.getCaseSensitive()));
            textOptionsDocument.put("diacriticSensitive", BsonBoolean.valueOf(textOptions.getDiacriticSensitive()));

            BsonDocument substringOptions = textOptions.getSubstringOptions();
            if (substringOptions != null) {
                textOptionsDocument.put("substring", substringOptions);
            }

            BsonDocument prefixOptions = textOptions.getPrefixOptions();
            if (prefixOptions != null) {
                textOptionsDocument.put("prefix", prefixOptions);
            }

            BsonDocument suffixOptions = textOptions.getSuffixOptions();
            if (suffixOptions != null) {
                textOptionsDocument.put("suffix", suffixOptions);
            }
            encryptOptionsBuilder.textOptions(textOptionsDocument);
        }
```

with (note: `getTextOptions()` is deprecated; suppress on the method or read via a local — use a `@SuppressWarnings("deprecation")` on `asMongoExplicitEncryptOptions`):

```java
        StringOptions stringOptions = resolveStringOptions(options);
        if (stringOptions != null) {
            BsonDocument stringOptionsDocument = new BsonDocument();
            stringOptionsDocument.put("caseSensitive", BsonBoolean.valueOf(stringOptions.getCaseSensitive()));
            stringOptionsDocument.put("diacriticSensitive", BsonBoolean.valueOf(stringOptions.getDiacriticSensitive()));

            BsonDocument substringOptions = stringOptions.getSubstringOptions();
            if (substringOptions != null) {
                stringOptionsDocument.put("substring", substringOptions);
            }

            BsonDocument prefixOptions = stringOptions.getPrefixOptions();
            if (prefixOptions != null) {
                stringOptionsDocument.put("prefix", prefixOptions);
            }

            BsonDocument suffixOptions = stringOptions.getSuffixOptions();
            if (suffixOptions != null) {
                stringOptionsDocument.put("suffix", suffixOptions);
            }
            encryptOptionsBuilder.textOptions(stringOptionsDocument);
        }
```

Then add the import `import com.mongodb.client.model.vault.TextOptions;` is already present — keep it. Add this private helper method above the private constructor (before `private EncryptOptionsHelper()`):

```java
    @SuppressWarnings("deprecation")
    @Nullable
    private static StringOptions resolveStringOptions(final EncryptOptions options) {
        StringOptions stringOptions = options.getStringOptions();
        if (stringOptions != null) {
            return stringOptions;
        }
        TextOptions textOptions = options.getTextOptions();
        if (textOptions == null) {
            return null;
        }
        return new StringOptions()
                .caseSensitive(textOptions.getCaseSensitive())
                .diacriticSensitive(textOptions.getDiacriticSensitive())
                .prefixOptions(textOptions.getPrefixOptions())
                .suffixOptions(textOptions.getSuffixOptions())
                .substringOptions(textOptions.getSubstringOptions());
    }
```

Add the import `import com.mongodb.lang.Nullable;` if not present.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :driver-core:test --tests "com.mongodb.internal.client.vault.EncryptOptionsHelperTest"`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
./gradlew :driver-core:spotlessApply
git add driver-core/src/main/com/mongodb/internal/client/vault/EncryptOptionsHelper.java \
        driver-core/src/test/unit/com/mongodb/internal/client/vault/EncryptOptionsHelperTest.java
git commit -m "feat(JAVA-6168): resolve stringOptions with textOptions fallback in EncryptOptionsHelper"
```

---

### Task 4: Refresh mongodb-crypt doc comments (TextPreview → String)

**Files:**
- Modify: `mongodb-crypt/src/main/com/mongodb/internal/crypt/capi/MongoExplicitEncryptOptions.java`

**Interfaces:**
- Consumes/Produces: none — doc-comment-only change. No signature or behavior change.

- [ ] **Step 1: Update doc comments**

In `MongoExplicitEncryptOptions.java`, update three Javadoc mentions of `"TextPreview"` to reflect the GA `"String"` algorithm. Replace `<p>Only applies when algorithm is "Indexed", "Range", or "TextPreview".</p>` (two occurrences, on `contentionFactor(...)` and `queryType(...)`) with `<p>Only applies when algorithm is "Indexed", "Range", or "String".</p>`, and replace `<p>Only applies when algorithm is "TextPreview".</p>` (on `textOptions(...)`) with `<p>Only applies when algorithm is "String".</p>`.

- [ ] **Step 2: Verify it compiles**

Run: `./gradlew :mongodb-crypt:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
./gradlew :mongodb-crypt:spotlessApply
git add mongodb-crypt/src/main/com/mongodb/internal/crypt/capi/MongoExplicitEncryptOptions.java
git commit -m "docs(JAVA-6168): refer to String algorithm in MongoExplicitEncryptOptions"
```

---

### Task 5: Rewrite the QE string explicit-encryption prose test to GA (spec test 27)

**Files:**
- Modify: `driver-sync/src/test/functional/com/mongodb/client/AbstractClientEncryptionTextExplicitEncryptionTest.java`

**Interfaces:**
- Consumes: `EncryptOptions` with `"String"` algorithm + `stringOptions(StringOptions)` (Tasks 1–2); test data files already present in the spec submodule (`encryptedFields-prefix-suffix.json`, `encryptedFields-prefix-suffix-ci-di.json`, `encryptedFields-prefix-suffix-preview.json`, `encryptedFields-substring.json`, `encryptedFields-substring-ci-di.json`, `keys/key1-document.json`).
- Produces: nothing (test only). The two concrete subclasses (`driver-sync/.../ClientEncryptionTextExplicitEncryptionTest.java` and `driver-reactive-streams/.../ClientEncryptionTextExplicitEncryptionTest.java`) need no change — they only implement `createMongoClient`/`createClientEncryption`.

Implements spec "27. String Explicit Encryption": algorithm `"String"` + `stringOptions`/`StringOptions`; cases 1–4 parameterized as `(prefix, prefix-suffix)` on server 9.0+ and `(prefixPreview, prefix-suffix-preview)` on server pre-9.0; cases 7–9 require server 9.0+; cases 5,6,10 run on 8.2+. An `autoEncryptedClient` (auto-encryption, no `bypassQueryAnalysis`) is added for cases 8–10.

- [ ] **Step 1: Run the existing test to confirm the starting state**

Run: `./gradlew :driver-sync:test --tests "com.mongodb.client.ClientEncryptionTextExplicitEncryptionTest"`
Expected: with a configured 8.2+ server and libmongocrypt 1.19.1, the *current* (commented-out-guards) test FAILS or errors, because it passes `new EncryptOptions("TextPreview")` which libmongocrypt 1.19.1 rejects (`unsupported algorithm string "textPreview"`). This confirms the rewrite is needed. (If no server is configured, the test is skipped — note this and proceed; the rewrite is verified in Step 3.)

- [ ] **Step 2: Replace the abstract test file**

Overwrite `driver-sync/src/test/functional/com/mongodb/client/AbstractClientEncryptionTextExplicitEncryptionTest.java` with:

```java
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

package com.mongodb.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.StringOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.connection.ServerVersion;
import com.mongodb.fixture.EncryptionFixture;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getMongoCryptVersion;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.fixture.EncryptionFixture.getKmsProviders;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

public abstract class AbstractClientEncryptionTextExplicitEncryptionTest {

    private static final ServerVersion REQUIRED_LIB_MONGOCRYPT_VERSION = new ServerVersion(asList(1, 19, 1));
    private boolean gaSupported;
    private MongoClient explicitEncryptedClient;
    private MongoClient autoEncryptedClient;
    private MongoDatabase explicitEncryptedDatabase;
    private MongoDatabase autoEncryptedDatabase;
    private ClientEncryption clientEncryption;
    private BsonBinary key1Id;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue("String explicit encryption tests disabled", hasEncryptionTestsEnabled());
        assumeTrue("Requires newer MongoCrypt version", getMongoCryptVersion().compareTo(REQUIRED_LIB_MONGOCRYPT_VERSION) >= 0);
        assumeTrue(serverVersionAtLeast(8, 2));
        assumeFalse(isStandalone());

        gaSupported = serverVersionAtLeast(9, 0);

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        BsonDocument key1Document = bsonDocumentFromPath("keys/key1-document.json");

        Map<String, Map<String, Object>> kmsProviders = getKmsProviders(EncryptionFixture.KmsProviderType.LOCAL);

        if (gaSupported) {
            createEncryptedCollection("prefix-suffix", "encryptedFields-prefix-suffix.json");
            createEncryptedCollection("prefix-suffix-ci-di", "encryptedFields-prefix-suffix-ci-di.json");
        } else {
            createEncryptedCollection("prefix-suffix-preview", "encryptedFields-prefix-suffix-preview.json");
        }
        createEncryptedCollection("substring", "encryptedFields-substring.json");
        createEncryptedCollection("substring-ci-di", "encryptedFields-substring-ci-di.json");

        MongoCollection<BsonDocument> dataKeysCollection = getMongoClient()
                .getDatabase(dataKeysNamespace.getDatabaseName())
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        dataKeysCollection.drop();
        dataKeysCollection.insertOne(key1Document);
        key1Id = key1Document.getBinary("_id");

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build());

        explicitEncryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(dataKeysNamespace.getFullName())
                        .kmsProviders(kmsProviders)
                        .bypassQueryAnalysis(true)
                        .build())
                .build());
        explicitEncryptedDatabase = explicitEncryptedClient.getDatabase(getDefaultDatabaseName())
                .withWriteConcern(WriteConcern.MAJORITY);

        autoEncryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(dataKeysNamespace.getFullName())
                        .kmsProviders(kmsProviders)
                        .build())
                .build());
        autoEncryptedDatabase = autoEncryptedClient.getDatabase(getDefaultDatabaseName())
                .withWriteConcern(WriteConcern.MAJORITY);

        // Seed the prefix-suffix collection(s) with an encrypted "foobarbaz" document.
        BsonBinary prefixSuffixSeed = clientEncryption.encrypt(new BsonString("foobarbaz"),
                new EncryptOptions("String")
                        .keyId(key1Id)
                        .contentionFactor(0L)
                        .stringOptions(new StringOptions()
                                .caseSensitive(true)
                                .diacriticSensitive(true)
                                .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))
                                .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
        if (gaSupported) {
            explicitEncryptedDatabase.getCollection("prefix-suffix")
                    .insertOne(new Document("_id", 0).append("encryptedText", prefixSuffixSeed));
        } else {
            explicitEncryptedDatabase.getCollection("prefix-suffix-preview")
                    .insertOne(new Document("_id", 0).append("encryptedText", prefixSuffixSeed));
        }

        // Seed the substring collection with an encrypted "foobarbaz" document.
        BsonBinary substringSeed = clientEncryption.encrypt(new BsonString("foobarbaz"),
                new EncryptOptions("String")
                        .keyId(key1Id)
                        .contentionFactor(0L)
                        .stringOptions(new StringOptions()
                                .caseSensitive(true)
                                .diacriticSensitive(true)
                                .substringOptions(BsonDocument.parse(
                                        "{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}"))));
        explicitEncryptedDatabase.getCollection("substring")
                .insertOne(new Document("_id", 0).append("encryptedText", substringSeed));
    }

    @Test
    @DisplayName("Case 1: can find a document by prefix")
    public void test1CanFindADocumentByPrefix() {
        String queryType = gaSupported ? "prefix" : "prefixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForPrefix("foo", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrStartsWith(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 2: can find a document by suffix")
    public void test2CanFindADocumentBySuffix() {
        String queryType = gaSupported ? "suffix" : "suffixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForSuffix("baz", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrEndsWith(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 3: assert no document found by prefix")
    public void test3AssertNoDocumentFoundByPrefix() {
        String queryType = gaSupported ? "prefix" : "prefixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForPrefix("baz", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrStartsWith(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 4: assert no document found by suffix")
    public void test4AssertNoDocumentFoundBySuffix() {
        String queryType = gaSupported ? "suffix" : "suffixPreview";
        String collection = gaSupported ? "prefix-suffix" : "prefix-suffix-preview";
        BsonBinary encrypted = encryptForSuffix("foo", queryType, true, true);
        Document result = explicitEncryptedDatabase.getCollection(collection)
                .find(encStrEndsWith(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 5: can find a document by substring")
    public void test5CanFindADocumentBySubstring() {
        BsonBinary encrypted = encryptForSubstring("bar", true, true);
        Document result = explicitEncryptedDatabase.getCollection("substring")
                .find(encStrContains(encrypted)).first();
        assertDocumentEquals(Document.parse("{ \"_id\": 0, \"encryptedText\": \"foobarbaz\" }"), result);
    }

    @Test
    @DisplayName("Case 6: assert no document found by substring")
    public void test6AssertNoDocumentFoundBySubstring() {
        BsonBinary encrypted = encryptForSubstring("qux", true, true);
        Document result = explicitEncryptedDatabase.getCollection("substring")
                .find(encStrContains(encrypted)).first();
        assertNull(result);
    }

    @Test
    @DisplayName("Case 7: assert `contentionFactor` is required")
    public void test7AssertContentionFactorIsRequired() {
        assumeTrue(gaSupported);
        EncryptOptions encryptOptions = new EncryptOptions("String")
                .keyId(key1Id)
                .queryType("prefix")
                .stringOptions(new StringOptions()
                        .caseSensitive(true)
                        .diacriticSensitive(true)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}")));
        MongoException exception = assertThrows(MongoException.class,
                () -> clientEncryption.encrypt(new BsonString("foo"), encryptOptions));
        assertTrue(exception.getMessage().contains("contention factor is required for string algorithm"));
    }

    @Test
    @DisplayName("Case 8: can find an auto-encrypted case-insensitively indexed document by prefix and suffix")
    public void test8AutoEncryptedCaseInsensitivePrefixAndSuffix() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .insertOne(new Document("encryptedText", "BingQiLin"));

        BsonBinary prefix = encryptForPrefix("bing", "prefix", false, false);
        Document byPrefix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrStartsWith(prefix)).first();
        assertEncryptedTextEquals("BingQiLin", byPrefix);

        BsonBinary suffix = encryptForSuffix("lin", "suffix", false, false);
        Document bySuffix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrEndsWith(suffix)).first();
        assertEncryptedTextEquals("BingQiLin", bySuffix);
    }

    @Test
    @DisplayName("Case 9: can find an auto-encrypted diacritic-insensitively indexed document by prefix and suffix")
    public void test9AutoEncryptedDiacriticInsensitivePrefixAndSuffix() {
        assumeTrue(gaSupported);
        autoEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .insertOne(new Document("encryptedText", "cafébarbäz"));

        BsonBinary prefix = encryptForPrefix("cafe", "prefix", false, false);
        Document byPrefix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrStartsWith(prefix)).first();
        assertEncryptedTextEquals("cafébarbäz", byPrefix);

        BsonBinary suffix = encryptForSuffix("baz", "suffix", false, false);
        Document bySuffix = explicitEncryptedDatabase.getCollection("prefix-suffix-ci-di")
                .find(encStrEndsWith(suffix)).first();
        assertEncryptedTextEquals("cafébarbäz", bySuffix);
    }

    @Test
    @DisplayName("Case 10: can find an auto-encrypted case-insensitively indexed document by substring")
    public void test10AutoEncryptedCaseInsensitiveSubstring() {
        autoEncryptedDatabase.getCollection("substring-ci-di")
                .insertOne(new Document("encryptedText", "FooBarBaz"));

        BsonBinary substring = encryptForSubstring("bar", false, false);
        Document result = explicitEncryptedDatabase.getCollection("substring-ci-di")
                .find(encStrContains(substring)).first();
        assertEncryptedTextEquals("FooBarBaz", result);
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY).drop();
        try (ClientEncryption ignored = this.clientEncryption;
             MongoClient ignored1 = this.explicitEncryptedClient;
             MongoClient ignored2 = this.autoEncryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    private void createEncryptedCollection(final String name, final String encryptedFieldsFile) {
        BsonDocument encryptedFields = bsonDocumentFromPath(encryptedFieldsFile);
        MongoDatabase database = getDefaultDatabase().withWriteConcern(WriteConcern.MAJORITY);
        database.getCollection(name).drop(new DropCollectionOptions().encryptedFields(encryptedFields));
        database.createCollection(name, new CreateCollectionOptions().encryptedFields(encryptedFields));
    }

    private BsonBinary encryptForPrefix(final String value, final String queryType,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType(queryType)
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .prefixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
    }

    private BsonBinary encryptForSuffix(final String value, final String queryType,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType(queryType)
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .suffixOptions(BsonDocument.parse("{strMaxQueryLength: 10, strMinQueryLength: 2}"))));
    }

    private BsonBinary encryptForSubstring(final String value,
            final boolean caseSensitive, final boolean diacriticSensitive) {
        return clientEncryption.encrypt(new BsonString(value), new EncryptOptions("String")
                .keyId(key1Id)
                .contentionFactor(0L)
                .queryType("substringPreview")
                .stringOptions(new StringOptions()
                        .caseSensitive(caseSensitive)
                        .diacriticSensitive(diacriticSensitive)
                        .substringOptions(BsonDocument.parse(
                                "{strMaxLength: 10, strMaxQueryLength: 10, strMinQueryLength: 2}"))));
    }

    private static Document encStrStartsWith(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrStartsWith",
                new Document("input", "$encryptedText").append("prefix", encrypted)));
    }

    private static Document encStrEndsWith(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrEndsWith",
                new Document("input", "$encryptedText").append("suffix", encrypted)));
    }

    private static Document encStrContains(final BsonBinary encrypted) {
        return new Document("$expr", new Document("$encStrContains",
                new Document("input", "$encryptedText").append("substring", encrypted)));
    }

    private static void assertDocumentEquals(final Document expectedDocument, final Document actualDocument) {
        actualDocument.remove("__safeContent__");
        assertEquals(expectedDocument, actualDocument);
    }

    private static void assertEncryptedTextEquals(final String expectedText, final Document actualDocument) {
        assertEquals(expectedText, actualDocument.getString("encryptedText"));
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        return getTestDocument("client-side-encryption/etc/data/" + path);
    }
}
```

- [ ] **Step 3: Run the prose test against a server**

Run (requires a configured non-standalone server 8.2+; set the URI):
`./gradlew :driver-sync:test --tests "com.mongodb.client.ClientEncryptionTextExplicitEncryptionTest" -Dorg.mongodb.test.uri="mongodb://localhost:27017"`
Expected: PASS. On a 9.0+ server, cases 1–10 run (GA query types). On an 8.2/8.3 server, cases 1–6 and 10 run with the preview path and cases 7–9 are skipped (`assumeTrue(gaSupported)`).

- [ ] **Step 4: Run the reactive variant**

Run: `./gradlew :driver-reactive-streams:test --tests "com.mongodb.reactivestreams.client.ClientEncryptionTextExplicitEncryptionTest" -Dorg.mongodb.test.uri="mongodb://localhost:27017"`
Expected: PASS (same gating).

- [ ] **Step 5: Commit**

```bash
./gradlew :driver-sync:spotlessApply
git add driver-sync/src/test/functional/com/mongodb/client/AbstractClientEncryptionTextExplicitEncryptionTest.java
git commit -m "test(JAVA-6168): rewrite QE string explicit-encryption prose test to GA"
```

---

### Task 6: Verify unified QE-Text spec tests run (not skipped) + flag CI matrix

**Files:**
- Inspect: `driver-sync/src/test/functional/com/mongodb/client/unified/ClientSideEncryptionTest.java`
- Inspect: `driver-sync/src/test/functional/com/mongodb/client/unified/UnifiedTestModifications.java`

**Interfaces:**
- Consumes/Produces: none — verification + optional skip-list edit only.

- [ ] **Step 1: Confirm the QE-Text unified tests are discovered**

Run: `./gradlew :driver-sync:test --tests "com.mongodb.client.unified.ClientSideEncryptionTest" -Dorg.mongodb.test.uri="mongodb://localhost:27017" --info` and confirm test names containing `QE-Text-prefix`, `QE-Text-suffix`, `QE-Text-prefixPreview`, `QE-Text-suffixPreview`, `QE-Text-substringPreview` appear (executed or skipped by `runOnRequirements`, not absent).
Expected: those display names are present in the run.

- [ ] **Step 2: Check for unintended skips**

Search the modification/skip registry for any entry that would disable these:

Run: `grep -ni "QE-Text\|text" driver-sync/src/test/functional/com/mongodb/client/unified/UnifiedTestModifications.java`
Expected: no skip that excludes the `QE-Text-*` files on supported server/libmongocrypt versions. If a stale skip exists (e.g. one added while the feature was preview/disabled), remove it and re-run Step 1.

- [ ] **Step 3: Record the CI matrix requirement**

The `QE-Text-prefixPreview`/`QE-Text-suffixPreview` unified tests declare `minServerVersion 8.2.0` / `maxServerVersion 8.99.99`. They only execute the auto-encryption preview path on a **pre-9.0** server. Confirm the encryption test matrix in `.evergreen/` runs against at least one 8.2/8.3 server in addition to 9.0+. **Do not modify `.evergreen/` config without human approval** — if the matrix lacks a pre-9.0 server, note it in the PR description and flag it to a human reviewer rather than editing CI config.

- [ ] **Step 4: Commit (only if a stale skip was removed in Step 2)**

```bash
./gradlew :driver-sync:spotlessApply
git add driver-sync/src/test/functional/com/mongodb/client/unified/UnifiedTestModifications.java
git commit -m "test(JAVA-6168): run QE-Text unified spec tests"
```

If no change was needed, skip the commit.

---

## Self-Review

**Spec coverage** (against `docs/superpowers/specs/2026-06-29-qe-prefix-suffix-ga-string-rename-design.md`):
- §4 new `StringOptions` + deprecated `TextOptions` → Task 1. ✅
- §4 `EncryptOptions.stringOptions`/deprecated `textOptions` + algorithm/queryType doc updates → Task 2. ✅
- §5 `EncryptOptionsHelper` resolution (stringOptions → textOptions fallback) → Task 3. ✅
- §5 mongodb-crypt doc-only refresh; no JNA change → Task 4. ✅
- §6 prose test 27 rewrite (cases 1–10, GA/preview gating, autoEncryptedClient) → Task 5. ✅
- §6 unified-test execution notes + CI pre-9.0 matrix → Task 6. ✅
- §7 language wrappers reuse driver-core classes (no wrapper change) → covered by Task 5 "Produces" note (concrete subclasses unchanged); Kotlin/Scala inherit automatically.
- Out-of-scope items (substring GA / preview removal / lookup prose-25) correctly excluded.

**Placeholder scan:** No TBD/TODO; all steps contain full code or exact commands.

**Type consistency:** `StringOptions` fluent setters/getters used identically in Tasks 1–3 and Task 5; `EncryptOptions.stringOptions(StringOptions)`/`getStringOptions()` and deprecated `textOptions(TextOptions)`/`getTextOptions()` consistent across Tasks 2, 3, 5; `MongoExplicitEncryptOptions.getTextOptions()` (internal type, unchanged) used in Task 3 test.
