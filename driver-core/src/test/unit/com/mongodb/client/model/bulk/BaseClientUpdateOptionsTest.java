package com.mongodb.client.model.bulk;

import com.mongodb.AssertUtils;
import org.junit.jupiter.api.Test;

class BaseClientUpdateOptionsTest {

    @Test
    void testAllSubInterfacesOverrideMethods() {
        AssertUtils.assertSubInterfaceReturnTypes("com.mongodb", BaseClientUpdateOptions.class);
    }
}
