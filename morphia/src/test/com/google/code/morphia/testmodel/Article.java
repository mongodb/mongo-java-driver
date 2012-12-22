package com.google.code.morphia.testmodel;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Reference;
import com.google.code.morphia.annotations.Property;
import com.google.code.morphia.testutil.TestEntity;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Olafur Gauti Gudmundsson
 */
@Entity("articles")
@SuppressWarnings("unchecked")
public class Article extends TestEntity {
	private static final long serialVersionUID = 1L;

    @Embedded
    private Map<String,Translation> translations;
	@Property
    private Map attributes;
    @Reference
    private Map<String,Article> related;

    public Article() {
        super();
        translations = new HashMap<String,Translation>();
        attributes = new HashMap<String,Object>();
        related = new HashMap<String,Article>();
    }

    public Map<String, Translation> getTranslations() {
        return translations;
    }

    public void setTranslations(Map<String, Translation> translations) {
        this.translations = translations;
    }

    public void setTranslation( String langCode, Translation t ) {
        translations.put(langCode, t);
    }

    public Translation getTranslation( String langCode ) {
        return translations.get(langCode);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public void setAttribute( String name, Object value ) {
        attributes.put(name, value);
    }

    public Object getAttribute( String name ) {
        return attributes.get(name);
    }

    public Map<String, Article> getRelated() {
        return related;
    }

    public void setRelated(Map<String, Article> related) {
        this.related = related;
    }

    public void putRelated(String name, Article a) {
        related.put(name, a);
    }

    public Article getRelated( String name ) {
        return related.get(name);
    }
}
