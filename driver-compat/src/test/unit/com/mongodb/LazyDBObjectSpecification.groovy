package com.mongodb

import org.bson.types.ObjectId

class LazyDBObjectSpecification extends FunctionalSpecification {

    def 'should understand DBRefs'() {
        given:
        byte[] bytes = [
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        ]

        when:
        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(collection))

        then:
        document.get('f') instanceof DBRef
        document.get('f') == new DBRef(database, 'a.b', new ObjectId('123456789012345678901234'))

    }
}
