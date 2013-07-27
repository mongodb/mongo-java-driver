package org.mongodb

import spock.lang.Specification
import spock.lang.Unroll

class IndexSpecification extends Specification {
    @Unroll
    def 'should generate index name #indexName for #index'() {
        expect:
        index.getName() == indexName;

        where:
        index                                              | indexName
        new Index('x')                                     | 'x_1'
        new Index('x', OrderBy.ASC)                        | 'x_1'
        new Index('x', OrderBy.DESC)                       | 'x_-1'
        new Index(new Index.GeoKey('x'))                   | 'x_2d'

        new Index(new Index.OrderedKey('x', OrderBy.ASC),
                  new Index.OrderedKey('y', OrderBy.ASC),
                  new Index.OrderedKey('a', OrderBy.ASC))  | 'x_1_y_1_a_1'

        new Index(new Index.GeoKey('x'),
                  new Index.OrderedKey('y', OrderBy.DESC)) | 'x_2d_y_-1'

    }

}
