package com.mongodb.client.model.bulk;

import com.mongodb.AssertUtils;
import org.junit.jupiter.api.Test;

class ClientDeleteManyOptionsTest {

    @Test
    void testAllSubInterfacesOverrideMethods() {
        AssertUtils.assertSubInterfaceReturnTypes("com.mongodb", BaseClientDeleteOptions.class);
    }
}