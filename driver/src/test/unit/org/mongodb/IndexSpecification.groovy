package org.mongodb

import spock.lang.Specification
import spock.lang.Unroll

class IndexSpecification extends Specification {
    @Unroll
    def 'should generate index name #indexName for #index'() {
        expect:
        index.getName() == indexName;

        where:
        index                                                       | indexName
        new Index.Builder().addKey('x').build()                     | 'x_1'
        new Index.Builder().addKey('x', OrderBy.ASC).build()        | 'x_1'
        new Index.Builder().addKey('x', OrderBy.DESC).build()       | 'x_-1'
        new Index.Builder().addKey(new Index.GeoKey('x')).build()   | 'x_2d'

        new Index.Builder().addKeys(
            new Index.OrderedKey('x', OrderBy.ASC),
            new Index.OrderedKey('y', OrderBy.ASC),
            new Index.OrderedKey('a', OrderBy.ASC)).build()         | 'x_1_y_1_a_1'

        new Index.Builder().addKeys(
            new Index.GeoKey('x'),
            new Index.OrderedKey('y', OrderBy.DESC)).build()        | 'x_2d_y_-1'

    }

    @Unroll
    def 'should support unique indexes'() {
        expect:
        index.toDocument().getBoolean("unique") == isUnique

        where:
        index                                                           | isUnique
        new Index.Builder().addKey('x').build()                         | null
        new Index.Builder().addKey('x').unique().build()                | true
        new Index.Builder().addKey('x').unique().unique(false).build()  | null
    }

    @Unroll
    def 'should support ttl indexes'() {
        expect:
        index.toDocument().getInteger("expireAfterSeconds") == seconds

        where:
        index                                                               | seconds
        new Index.Builder().addKey('x').build()                             | null
        new Index.Builder().addKey('x').build()                             | null
        new Index.Builder().addKey('x').build()                             | null
        new Index.Builder().addKey('x').expireAfterSeconds(1000).build()    | 1000
        new Index.Builder().addKey('x').expireAfterSeconds(1000)
            .expireAfterSeconds(-1).build()                                 | null
    }
}
