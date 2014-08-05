package org.bson.codecs.jackson;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;

/**
 * Created by guo on 7/28/14.
 */
public class JacksonBsonModule extends Module{

    @Override
    public String getModuleName() {
        return "JacksonBsonModule";
    }

    @Override
    public Version version() {
        return new Version(1, 0, 0, "", "org.bson.codecs.jackson", "mongodb");
    }

    @Override
    public void setupModule(SetupContext context) {
        context.addSerializers(new JacksonBsonSerializers());
        context.addDeserializers(new JacksonBsonDeserializers());
    }
}
