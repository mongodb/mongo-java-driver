package org.bson.internal

import org.bson.UuidRepresentation
import spock.lang.Specification
import spock.lang.Unroll

class UuidHelperSpecification extends Specification {

    @Unroll
    def 'should encode different types of UUID'() {
        given:
        def expectedUuid = UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09')

        expect:
        bytes == UuidHelper.encodeUuidToBinary(expectedUuid, uuidRepresentation)

        where:
        bytes                                                   | uuidRepresentation
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] | UuidRepresentation.JAVA_LEGACY
        [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9] | UuidRepresentation.STANDARD
        [8, 7, 6, 5, 4, 3, 2, 1, 16, 15, 14, 13, 12, 11, 10, 9] | UuidRepresentation.PYTHON_LEGACY
        [5, 6, 7, 8, 3, 4, 1, 2, 16, 15, 14, 13, 12, 11, 10, 9] | UuidRepresentation.C_SHARP_LEGACY
    }

    @Unroll
    def 'should decode different types of UUID'() {
        given:
        byte[] expectedBytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]

        expect:
        uuid == UuidHelper.decodeBinaryToUuid(expectedBytes, (byte) type, uuidRepresentation)

        where:
        uuid                                                          | type | uuidRepresentation
        UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09') | 3    | UuidRepresentation.JAVA_LEGACY
        UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10') | 4    | UuidRepresentation.STANDARD
        UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10') | 3    | UuidRepresentation.PYTHON_LEGACY
        UUID.fromString('04030201-0605-0807-090a-0b0c0d0e0f10') | 3    | UuidRepresentation.C_SHARP_LEGACY
    }
}
