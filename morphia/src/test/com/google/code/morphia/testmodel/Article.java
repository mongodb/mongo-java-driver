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

package com.google.code.morphia.testmodel;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.testutil.TestEntity;

import java.util.HashMap;
import java.util.Map;

@Entity("articles")
@SuppressWarnings("unchecked")
public class Article extends TestEntity {
    private static final long serialVersionUID = 1L;

    @Embedded
    private Map<String, Translation> translations;
    @Property
    private Map<String, Object> attributes;
    @Reference
    private Map<String, Article> related;

    public Article() {
        super();
        translations = new HashMap<String, Translation>();
        attributes = new HashMap<String, Object>();
        related = new HashMap<String, Article>();
    }

    public Map<String, Translation> getTranslations() {
        return translations;
    }

    public void setTranslations(final Map<String, Translation> translations) {
        this.translations = translations;
    }

    public void setTranslation(final String langCode, final Translation t) {
        translations.put(langCode, t);
    }

    public Translation getTranslation(final String langCode) {
        return translations.get(langCode);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(final Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public void setAttribute(final String name, final Object value) {
        attributes.put(name, value);
    }

    public Object getAttribute(final String name) {
        return attributes.get(name);
    }

    public Map<String, Article> getRelated() {
        return related;
    }

    public void setRelated(final Map<String, Article> related) {
        this.related = related;
    }

    public void putRelated(final String name, final Article a) {
        related.put(name, a);
    }

    public Article getRelated(final String name) {
        return related.get(name);
    }
}
