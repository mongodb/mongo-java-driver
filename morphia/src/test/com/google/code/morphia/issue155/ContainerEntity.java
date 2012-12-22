package com.google.code.morphia.issue155;

import com.google.code.morphia.testutil.TestEntity;

class ContainerEntity extends TestEntity {
    private static final long serialVersionUID = 1L;
    
    Bar foo = EnumBehindAnInterface.A;
}