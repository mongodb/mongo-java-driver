package org.mongodb

import spock.lang.Specification
import spock.lang.Unroll

class IndexSpecification extends Specification {
    @Unroll
    def 'should generate index name #indexName for #index'() {
        expect:
        index.getName() == indexName;

        where:
        index                                                   | indexName
        Index.builder().addKey('x').build()                     | 'x_1'
        Index.builder().addKey('x', OrderBy.ASC).build()        | 'x_1'
        Index.builder().addKey('x', OrderBy.DESC).build()       | 'x_-1'
        Index.builder().addKey(new Index.GeoKey('x')).build()   | 'x_2d'

        Index.builder().addKeys(
            new Index.OrderedKey('x', OrderBy.ASC),
            new Index.OrderedKey('y', OrderBy.ASC),
            new Index.OrderedKey('a', OrderBy.ASC)).build()     | 'x_1_y_1_a_1'

        Index.builder().addKeys(
            new Index.GeoKey('x'),
            new Index.OrderedKey('y', OrderBy.DESC)).build()    | 'x_2d_y_-1'

    }

    @Unroll
    def 'should support unique indexes'() {
        expect:
        index.toDocument().getBoolean("unique") == isUnique

        where:
        index                                                       | isUnique
        Index.builder().addKey('x').build()                         | null
        Index.builder().addKey('x').unique().build()                | true
        Index.builder().addKey('x').unique().unique(false).build()  | null
    }

    @Unroll
    def 'should support ttl indexes'() {
        expect:
        index.toDocument().getInteger("expireAfterSeconds") == seconds

        where:
        index                                                           | seconds
        Index.builder().addKey('x').build()                             | null
        Index.builder().addKey('x').expireAfterSeconds(1000).build()    | 1000
        Index.builder().addKey('x').expireAfterSeconds(1000)
            .expireAfterSeconds(-1).build()                             | null
    }
    
    @Unroll
    def 'should support dropping duplicates'() {
        expect:
        index.toDocument().getBoolean("dropDups") == dropDups

        where:
        index                                                           | dropDups
        Index.builder().addKey('x').build()                             | null
        Index.builder().addKey('x').dropDups().build()                  | true
        Index.builder().addKey('x').dropDups(false).build()             | null
        Index.builder().addKey('x').dropDups().dropDups(false).build()  | null
    }
    
    @Unroll
    def 'should support unknown attributes'() {
        expect:
        index.toDocument().get("extra") == extra

        where:
        index                                                           | extra
        Index.builder().addKey('x').build()                             | null
        Index.builder().addKey('x').extra("extra", "special").build()   | "special"
    }
}
