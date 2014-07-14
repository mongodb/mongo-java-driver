package org.bson.codecs.jackson.deserializers;

import com.fasterxml.jackson.databind.module.SimpleDeserializers;

import java.util.Date;

/**
 * Created by guo on 7/28/14.
 */
public class JacksonBsonDeserializers extends SimpleDeserializers {
    private static final long serialVersionUID = 261492073508623840L;

    /**
     * Default constructor
     */
    public JacksonBsonDeserializers() {
        addDeserializer(Date.class, new JacksonDateDeserializer());
    }
}