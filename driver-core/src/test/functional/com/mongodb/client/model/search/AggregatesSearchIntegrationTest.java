/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.client.model.search;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.time.Month;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isAtlasSearchTest;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceWith;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Projections.computedSearchMeta;
import static com.mongodb.client.model.Projections.metaSearchHighlights;
import static com.mongodb.client.model.Projections.metaSearchScore;
import static com.mongodb.client.model.Projections.metaVectorSearchScore;
import static com.mongodb.client.model.search.FuzzySearchOptions.fuzzySearchOptions;
import static com.mongodb.client.model.search.SearchCollector.facet;
import static com.mongodb.client.model.search.SearchCount.lowerBound;
import static com.mongodb.client.model.search.SearchCount.total;
import static com.mongodb.client.model.search.SearchFacet.dateFacet;
import static com.mongodb.client.model.search.SearchFacet.numberFacet;
import static com.mongodb.client.model.search.SearchFacet.stringFacet;
import static com.mongodb.client.model.search.SearchHighlight.paths;
import static com.mongodb.client.model.search.SearchOperator.autocomplete;
import static com.mongodb.client.model.search.SearchOperator.compound;
import static com.mongodb.client.model.search.SearchOperator.dateRange;
import static com.mongodb.client.model.search.SearchOperator.equalsNull;
import static com.mongodb.client.model.search.SearchOperator.exists;
import static com.mongodb.client.model.search.SearchOperator.in;
import static com.mongodb.client.model.search.SearchOperator.moreLikeThis;
import static com.mongodb.client.model.search.SearchOperator.near;
import static com.mongodb.client.model.search.SearchOperator.numberRange;
import static com.mongodb.client.model.search.SearchOperator.queryString;
import static com.mongodb.client.model.search.SearchOperator.regex;
import static com.mongodb.client.model.search.SearchOperator.phrase;
import static com.mongodb.client.model.search.SearchOperator.text;
import static com.mongodb.client.model.search.SearchOperator.wildcard;
import static com.mongodb.client.model.search.SearchOptions.searchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static com.mongodb.client.model.search.SearchScore.boost;
import static com.mongodb.client.model.search.SearchScore.constant;
import static com.mongodb.client.model.search.SearchScore.function;
import static com.mongodb.client.model.search.SearchScoreExpression.addExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.constantExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.gaussExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.log1pExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.logExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.multiplyExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.pathExpression;
import static com.mongodb.client.model.search.SearchScoreExpression.relevanceExpression;
import static com.mongodb.client.model.search.VectorSearchOptions.approximateVectorSearchOptions;
import static com.mongodb.client.model.search.VectorSearchOptions.exactVectorSearchOptions;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * These tests require the <a href="https://www.mongodb.com/docs/atlas/sample-data/">sample data</a>
 * and the following Atlas Search indices:
 * <table>
 *  <thead>
 *      <tr>
 *          <th>Namespace</th>
 *          <th>Index name</th>
 *          <th>Field mappings</th>
 *      </tr>
 *  </thead>
 *  <tbody>
 *      <tr>
 *          <td>{@code sample_mflix.movies}</td>
 *          <td>{@code default}</td>
 *          <td><pre>{@code
 *            {
 *              "mappings": {
 *                "dynamic": true,
 *                "fields": {
 *                  "fullplot": {
 *                    "type": "stringFacet"
 *                  },
 *                  "released": {
 *                    "type": "dateFacet"
 *                  },
 *                  "title": [
 *                    {
 *                      "multi": {
 *                        "keyword": {
 *                          "analyzer": "lucene.keyword",
 *                          "searchAnalyzer": "lucene.keyword",
 *                          "type": "string"
 *                        }
 *                      },
 *                      "type": "string"
 *                    },
 *                    {
 *                      "type": "autocomplete"
 *                    }
 *                  ],
 *                  "tomatoes": {
 *                    "fields": {
 *                      "dvd": {
 *                        "type": "date"
 *                      },
 *                      "viewer": {
 *                        "fields": {
 *                          "meter": {
 *                            "type": "numberFacet"
 *                          }
 *                        },
 *                        "type": "document"
 *                      }
 *                    },
 *                    "type": "document"
 *                  }
 *                }
 *              },
 *              "storedSource": {
 *                "include": [
 *                  "plot"
 *                ]
 *              }
 *            }
 *          }</pre></td>
 *      </tr>
 *      <tr>
 *          <td>{@code sample_airbnb.listingsAndReviews}</td>
 *          <td>{@code default}</td>
 *          <td><pre>{@code
 *            {
 *              "mappings": {
 *                "dynamic": true,
 *                "fields": {
 *                  "address": {
 *                    "fields": {
 *                      "location": {
 *                        "type": "geo"
 *                      }
 *                    },
 *                    "type": "document"
 *                  }
 *                }
 *              }
 *            }
 *          }</pre></td>
 *      </tr>
 *      <tr>
 *          <td>{@code sample_mflix.embedded_movies}</td>
 *          <td>{@code sample_mflix__embedded_movies}</td>
 *          <td><pre>{@code
 *            {
 *              "mappings": {
 *                "dynamic": true,
 *                "fields": {
 *                  "plot_embedding": {
 *                    "dimensions": 1536,
 *                    "similarity": "cosine",
 *                    "type": "knnVector"
 *                  }
 *                }
 *              }
 *            }
 *          }</pre></td>
 *      </tr>
 *  </tbody>
 * </table>
 */
final class AggregatesSearchIntegrationTest {
    private static final MongoNamespace MFLIX_MOVIES_NS = new MongoNamespace("sample_mflix", "movies");
    private static final MongoNamespace MFLIX_EMBEDDED_MOVIES_NS = new MongoNamespace("sample_mflix", "embedded_movies");
    private static final MongoNamespace AIRBNB_LISTINGS_AND_REVIEWS_NS = new MongoNamespace("sample_airbnb", "listingsAndReviews");
    private static final List<Double> QUERY_VECTOR = unmodifiableList(asList(-0.0072121937, -0.030757688, 0.014948666, -0.018497631, -0.019035352, 0.028149737, -0.0019593239,  -0.02012424, -0.025649332, -0.007985169, 0.007830574, 0.023726976, -0.011507247, -0.022839734, 0.00027999343, -0.010431803, 0.03823202, -0.025756875, -0.02074262, -0.0042883316, -0.010841816, 0.010552791, 0.0015266258, -0.01791958, 0.018430416, -0.013980767, 0.017247427, -0.010525905, 0.0126230195, 0.009255537, 0.017153326, 0.008260751, -0.0036060968, -0.019210111, -0.0133287795, -0.011890373, -0.0030599732, -0.0002904958, -0.001310697, -0.020715732, 0.020890493, 0.012428096, 0.0015837587, -0.006644225, -0.028499257, -0.005098275, -0.0182691, 0.005760345, -0.0040665213, 0.00075491105, 0.007844017, 0.00040791242, 0.0006780336, 0.0027037326, -0.0041370974, -0.022275126, 0.004775642, -0.0045235846, -0.003659869, -0.0020567859, 0.021602973, 0.01010917, -0.011419867, 0.0043689897, -0.0017946466, 0.000101610516, -0.014061426, -0.002626435, -0.00035540052, 0.0062174085, 0.020809835, 0.0035220778, -0.0071046497, -0.005041142, 0.018067453, 0.012569248, -0.021683631, 0.020245226, 0.017247427, 0.017032338, 0.01037131, -0.036296222, -0.026334926, 0.041135717, 0.009625221, 0.032155763, -0.025057837, 0.027827105, -0.03323121, 0.0055721425, 0.005716655, 0.01791958, 0.012078577, -0.011117399, -0.0016005626, -0.0033254733, -0.007702865, 0.034306653, 0.0063854465, -0.009524398, 0.006069535, 0.012696956, -0.0042883316, -0.013167463, -0.0024667988, -0.02356566, 0.00052721944, -0.008858967, 0.039630096, -0.0064593833, -0.0016728189, -0.0020366213, 0.00622413, -0.03739855, 0.0028616884, -0.0102301575, 0.017717933, -0.0041068504, -0.0060896995, -0.01876649, 0.0069903834, 0.025595559, 0.029762903, -0.006388807, 0.017247427, 0.0022080203, -0.029117636, -0.029870447, -0.0049739266, -0.011809715, 0.023243025, 0.009510955, 0.030004878, 0.0015837587, -0.018524516, 0.007931396, -0.03589293, 0.013590919, -0.026361812, 0.002922182, 0.025743432, 0.014894894, 0.0012989342, -0.0016232478, 0.006251016, 0.029789789, -0.004664737, 0.017812036, -0.013436324, -0.0102301575, 0.016884465, -0.017220542, 0.010156221, 0.00014503786, 0.03933435, 0.018658947, 0.016897907, 0.0076961434, -0.029843561, -0.02021834, 0.015056211, 0.01002179, -0.0031994449, -0.03796316, -0.008133043, 0.03707592, 0.032128878, 9.483648E-05, 0.0017627194, -0.0007544909, 0.006647586, 0.020903936, -0.032559056, 0.025272924, -0.012804501, 0.019210111, 0.0022987607, 0.013301893, -0.0047218697, -0.022853177, -0.02162986, 0.006788738, 0.0092286505, 0.024184039, -0.015419173, -0.006479548, -0.00180977, 0.0060728956, -0.0030919004, 0.0022449887, -0.004046357, 0.012663349, -0.028579915, 0.0047722813, -0.6775295, -0.018779935, -0.018484188, -0.017449073, -0.01805401, 0.026630674, 0.008018777, 0.013436324, -0.0034683058, 0.00070912065, -0.005027699, 0.009658828, -0.0031792803, -0.010478854, 0.0034951917, -0.011594627, 0.02441257, -0.042533796, -0.012414653, 0.006261098, -0.012266779, 0.026630674, -0.017852364, -0.02184495, 0.02176429, 0.019263884, 0.00984031, -0.012609577, -0.01907568, -0.020231783, -0.002886894, 0.02706085, -0.0042345594, 0.02265153, 0.05769755, 0.021522315, -0.014195856, 0.011144285, 0.0038077426, 0.024573887, -0.03578539, -0.004476534, 0.016521502, -0.019815048, 0.00071836275, 0.008173372, 0.013436324, 0.021885278, -0.0147604635, -0.021777734, 0.0052595916, -0.011668564, -0.02356566, -0.0049974523, 0.03473683, -0.0255149, 0.012831387, -0.009658828, -0.0031036632, -0.001386314, -0.01385978, 0.008294359, -0.02512505, -0.0012308789, 0.008711093, 0.03610802, 0.016225755, 0.014034539, 0.0032431346, -0.017852364, 0.017906137, 0.005787231, -0.03514012, 0.017207097, -0.0019542826, -0.010189828, 0.010808208, -0.017408744, -0.0074944976, 0.011009854, 0.00887241, 0.009652107, -0.0062409337, 0.009766373, 0.009759651, -0.0020819916, -0.02599885, 0.0040665213, 0.016064439, -0.019035352, -0.013604362, 0.020231783, -0.025272924, -0.01196431, -0.01509654, 0.0010233518, -0.00869765, -0.01064017, 0.005249509, -0.036807057, 0.00054570363, 0.0021777733, -0.009302587, -0.00039362916, 0.011386259, 0.013382551, 0.03046194, 0.0032380936, 0.037801843, -0.036807057, -0.006244295, 0.002392862, -0.01346321, -0.008953068, -0.0025861058, -0.022853177, 0.018242212, -0.0031624765, 0.009880639, -0.0017341529, 0.0072054723, 0.014693249, 0.026630674, 0.008435511, -0.012562525, 0.011581183, -0.0028768117, -0.01059312, -0.027746446, 0.0077969665, 2.468059E-05, -0.011151006, 0.0152712995, -0.01761039, 0.023256468, 0.0076625356, 0.0026163526, -0.028795004, 0.0025877862, -0.017583502, -0.016588718, 0.017556617, 0.00075491105, 0.0075885993, -0.011722336, -0.010620005, -0.017274313, -0.008025498, -0.036376882, 0.009457182, -0.007265966, -0.0048663826, -0.00494368, 0.003616179, 0.0067820163, 0.0033775652, -0.016037554, 0.0043320213, -0.007978448, -0.012925488, 0.029413383, -0.00016583256, -0.018040568, 0.004180787, -0.011453475, -0.013886666, -0.0072121937, 0.006486269, 0.008005333, -0.01412864, -0.00061796, -0.025635887, -0.006630782, 0.02074262, -0.007192029, 0.03906549, -0.0030885397, -0.00088976155, -0.022033151, -0.008758144, 0.00049361185, 0.009342916, -0.014988995, -0.008704372, 0.014276514, -0.012300386, -0.0020063745, 0.030892119, -0.010532626, 0.019653732, 0.0028583275, 0.006163636, 0.0071517, -0.017489402, -0.008448954, -0.004352186, 0.013201071, 0.01090231, 0.0004110631, 0.03306989, 0.006916447, 0.002922182, 0.023888292, -0.009067334, 0.012434817, -0.051298663, 0.016279528, -0.02741037, 0.026227381, -0.005182294, 0.008153207, -0.026603786, 0.0045571923, 0.018067453, 0.038016934, 0.028042194, 0.0077431942, 0.015499831, -0.020298999, 0.0013123773, -0.021334114, -0.026281154, -0.0012720482, -0.0045571923, 0.006086339, 0.0028952959, -0.003041489, 0.007931396, -0.0005406625, -0.023444671, -0.0038715971, 0.0070374343, -0.0019979726, 0.024089938, 0.0020903936, -0.024210924, 0.007319738, -0.005995598, 0.032478396, 0.020998036, 0.01654839, 0.033876475, 0.025098165, 0.021132467, -0.017099554, -0.013516982, 0.01306664, 0.010525905, -0.02335057, -0.013543868, -0.03583916, 0.021172797, -0.033607613, -0.0036094578, -0.007911232, -0.0054578763, 0.013227956, 0.00993441, 0.025810648, 0.02255743, -0.013678298, 0.012273501, 0.00040497174, 0.0019072321, 0.0008170851, 0.01540573, 0.015580489, 0.005239427, 0.003989224, -0.013254843, 0.024708318, 0.0046680975, -0.034360424, -0.0041942303, 0.0077095865, -0.0053503322, -0.024399128, -0.02644247, 0.0062476555, 0.021885278, -0.0010922474, -0.014209299, 0.018295985, 0.0135640325, 0.0033842868, 0.0017812036, 0.004735313, 0.006486269, -0.008072549, 0.009551284, 0.007938119, 0.0101696635, 0.021750847, 0.014034539, 0.0071449787, -0.008448954, 0.010841816, -0.008274195, -0.014531932, -0.0024785616, 0.0018601815, 0.009564727, -0.011130841, -0.020581303, 0.012985982, 0.019976366, -0.030542599, -0.021818062, -0.018551402, -0.0092286505, -0.024385685, 0.0036901159, -0.0061367503, -0.00034048714, -0.007057599, -0.014558818, -0.022221355, 0.023377456, 0.026119838, -0.0008813597, 0.004520224, 0.0027843907, -0.022382671, 0.0018248934, 0.13313992, 0.013685021, -6.170148E-05, 0.015876237, 0.005417547, -0.008314524, -0.019169783, -0.016494617, 0.016844137, -0.0046412116, 0.024305027, -0.027827105, 0.023162367, 0.0143034, -0.0029893972, -0.014626034, -0.018215327, 0.0073264595, 0.024331912, -0.0070777633, -0.0004259765, -0.00042345593, -0.0034262962, -0.00423792, -0.016185427, -0.017946465, -5.9706024E-05, 0.016467731, -0.014773907, -0.022664975, -0.009322752, -0.027585128, 0.0020651878, -0.010532626, -0.010546069, 0.009174879, -0.0011098915, 0.026469355, 0.022006266, -0.013039754, 0.023458114, 0.005481402, -0.00050705485, -0.012092019, 0.0055990284, -0.007057599, -0.012266779, 0.03253217, 0.007071042, -0.01699201, 0.06597847, -0.013436324, 0.0070038266, -0.009981461, 0.024829306, 0.0067383265, 0.0056292755, 0.0018534599, -0.020057024, 0.011735778, 0.0025491375, -0.022194467, 0.0012468424, -0.0051621296, -0.018457301, -0.008509448, -0.011594627, -0.0152712995, -0.001858501, -0.014921781, -0.0056696045, -0.0066979975, -0.02008391, 0.0040093884, 0.032935463, -0.0032935461, -0.0074205613, -0.014088311, -0.0014762144, -0.011218221, 0.011984475, -0.01898158, -0.027208723, -0.008072549, 0.010942639, 0.0183632, 0.04148524, -0.0009922648, -0.017086111, 0.013483374, 0.019841935, 0.024264697, 0.011601348, -0.0077431942, -0.020258669, -0.005770427, 0.013429603, -0.011554297, -0.012831387, -1.4752561E-06, 0.011594627, -0.012683514, -0.012824666, 0.02180462, 0.011023297, 0.012468425, -0.0029860365, -0.0076289284, -0.021293784, 0.005068028, 0.017812036, 0.0007708746, -0.008684208, 0.0048126103, -0.0076558143, 0.019169783, -0.0076558143, 0.028579915, -0.011574462, -0.03196756, -0.0011334168, -0.030219967, 0.023901735, 0.014021097, -0.016776921, 0.0030045207, -0.0019257163, -0.023579102, 0.004197591, 0.00012497831, -0.016803807, 0.01915634, -0.010472132, -0.042130504, -0.038016934, -0.007702865, -0.0025861058, -0.010512462, -0.013537147, -0.013382551, -0.0036397045, 0.0053032814, 0.0046277684, -0.021952493, -0.016588718, -0.031886905, 0.0058208387, -0.00043689896, -0.01337583, 0.018349757, 0.015244413, 0.00900684, -0.017677605, 0.01523097, 0.010337702, -0.024426013, -0.021965936, -0.014182413, 0.008596827, 0.029628472, 0.058611676, -0.015446059, 0.021374442, -0.0095042335, 0.00091748784, 0.021132467, -0.011285436, -0.0035724894, -0.027907763, 0.027302826, 0.004184148, 0.026281154, -0.0026802071, -0.015163755, 0.005699851, 0.023122039, 0.0075415485, -0.020057024, -0.0109359175, -0.018309427, 0.017529732, 0.0020685487, -0.012441538, 0.0023239665, 0.012038247, -0.017543174, 0.029332725, 0.01399421, -0.0092488155, -1.0607403E-05, 0.019371428, -0.0315105, 0.023471557, -0.009430297, 0.00022097006, 0.013301893, -0.020110795, -0.0072928523, 0.007649093, 0.011547576, 0.026805433, -0.01461259, -0.018968137, -0.0104250815, 0.0005646079, 0.031456728, -0.0020147765, -0.024224367, 0.002431511, -0.019371428, -0.025017507, -0.02365976, -0.004318578, -0.04457714, 0.0029826758, -0.020473758, -0.016118212, -0.00068181445, -0.03446797, -0.020715732, -0.04256068, -0.013792564, 0.013873223, 0.011413146, -0.002419748, 0.0123877665, -0.0011115718, 0.007978448, 0.021441657, 0.004405958, 0.0042480025, 0.022920392, -0.0067920987, 0.011083791, -0.017529732, -0.03659197, -0.0066005355, -0.023888292, -0.016521502, 0.009591613, -0.0008590946, 0.013846337, -0.021092137, -0.012562525, -0.0028415236, 0.02882189, 5.3378342E-05, -0.006943333, -0.012226449, -0.035570297, -0.024547001, 0.022355784, -0.018416973, 0.014209299, 0.010035234, 0.0046916227, 0.009672271, -0.00067635323, -0.024815861, 0.0007049197, 0.0017055863, -0.0051251613, 0.0019391594, 0.027665788, -0.007306295, -0.013369109, 0.006308149, 0.009699157, 0.000940173, 0.024842748, 0.017220542, -0.0053032814, -0.008395182, 0.011359373, 0.013214514, 0.0062711807, 0.004110211, -0.019277327, -0.01412864, -0.009322752, 0.007124814, 0.0035119955, -0.024036165, -0.012831387, -0.006734966, -0.0019694061, -0.025367027, -0.006630782, 0.016010666, 0.0018534599, -0.0030717358, -0.017717933, 0.008489283, 0.010875423, -0.0028700903, 0.0121323485, 0.004930237, 0.009947853, -0.02992422, 0.021777734, 0.00015081417, 0.010344423, 0.0017543174, 0.006166997, -0.0015467904, 0.010089005, 0.0111711705, -0.010740994, -0.016965123, -0.006771934, 0.014464716, 0.007192029, -0.0006175399, -0.010855259, -0.003787578, 0.015647706, 0.01002179, -0.015378844, -0.01598378, 0.015741806, -0.0039119264, -0.008422068, 0.03253217, -0.019210111, -0.014975552, 0.0025810648, 0.0035556855, 8.449164E-05, -0.034172222, -0.006395529, -0.0036867552, 0.020769505, 0.009766373, -0.017543174, -0.013557311, 0.0031994449, -0.0014577302, 0.01832287, -0.009907524, -0.024654545, 0.0049940916, 0.016965123, 0.004476534, 0.022261683, -0.009369803, 0.0015308268, -0.010102449, -0.001209874, -0.023807634, -0.008348132, -0.020312442, 0.030892119, -0.0058309208, -0.005128522, -0.02437224, 0.01478735, -0.011016576, -0.010290652, -0.00503106, 0.016884465, 0.02132067, -0.014236185, -0.004903351, 0.01902191, 0.0028179984, 0.019505858, -0.021535758, -0.0038514326, 0.0112115, 0.0038682362, 0.003217929, -0.0012770894, -0.013685021, -0.008381739, 0.0025256122, 0.029386498, 0.018645504, 0.005323446, -0.0032784226, -0.0043253, 0.0007998612, 0.019949479, 0.025770318, -0.0030868594, 0.018968137, -0.010236879, -0.005370497, -0.024748646, -0.014047982, 0.005760345, -0.03610802, 0.0042009517, -0.0034817487, 0.003385967, 0.006560206, -0.006294706, -0.02400928, -0.006140111, -0.0017980073, -0.012481867, -0.0033960494, -0.00097210024, 0.014061426, -0.017596947, -0.023202697, 0.0028499255, -0.016010666, -0.028149737, 0.0024752007, -0.018941252, 0.0056158323, -0.012912045, 0.0054410724, 0.003054932, 0.019559631, -0.0048932685, -0.007823853, -0.017099554, 0.025662774, 0.02572999, 0.004379072, -0.010223436, 0.0031036632, -0.011755943, -0.025622444, -0.030623257, 0.019895706, -0.02052753, -0.006637504, -0.001231719, -0.013980767, -0.02706085, -0.012071854, -0.0041370974, -0.008885853, 0.0001885177, 0.2460615, -0.009389968, -0.010714107, 0.0326666, 0.0009561366, 0.022624645, 0.009793258, 0.019452088, -0.004493338, -0.007097928, -0.0022298652, 0.012401209, -0.0036229007, -0.00023819396, -0.017502844, -0.014209299, -0.030542599, -0.004863022, 0.005128522, -0.03081146, 0.02118624, -0.0042177555, 0.0032448152, -0.019936036, 0.015311629, 0.0070508774, -0.02021834, 0.0016148458, 0.04317906, 0.01385978, 0.004211034, -0.02534014, -0.00030309867, -0.011930703, -0.00207527, -0.021643303, 0.01575525, -0.0042883316, 0.0069231684, 0.017946465, 0.03081146, 0.0043857936, 3.646951E-05, -0.0214551, 0.0089933975, 0.022785962, -0.008106156, 0.00082884775, -0.0006717322, -0.0025457768, -0.017059224, -0.035113234, 0.054982055, 0.021266898, -0.0071046497, -0.012636462, 0.016965123, 0.01902191, -0.0061737187, 0.00076247274, 0.0002789432, 0.030112421, -0.0026768465, 0.0015207445, -0.004926876, 0.0067551304, -0.022624645, 0.0005003333, 0.0035523248, -0.0041337362, 0.011634956, -0.0183632, -0.02820351, -0.0061737187, -0.022355784, -0.03796316, 0.041888528, 0.019626847, 0.02211381, 0.001474534, 0.0037640526, 0.0085228905, 0.013140577, 0.012616298, -0.010599841, -0.022920392, 0.011278715, -0.011493804, -0.0044966987, -0.028741231, 0.015782135, -0.011500525, -0.00027621258, -0.0046378504, -0.003280103, 0.026993636, 0.0109359175, 0.027168395, 0.014370616, -0.011890373, -0.020648519, -0.03465617, 0.001964365, 0.034064677, -0.02162986, -0.01081493, 0.014397502, 0.008038941, 0.029789789, -0.012044969, 0.0038379894, -0.011245107, 0.0048193317, -0.0048563, 0.0142899575, 0.009779816, 0.0058510853, -0.026845763, 0.013281729, -0.0005818318, 0.009685714, -0.020231783, -0.004197591, 0.015593933, -0.016319858, -0.019492416, -0.008314524, 0.014693249, 0.013617805, -0.02917141, -0.0052058194, -0.0061838008, 0.0072726877, -0.010149499, -0.019035352, 0.0070374343, -0.0023138842, 0.0026583623, -0.00034111727, 0.0019038713, 0.025945077, -0.014693249, 0.009820145, -0.0037506097, 0.00041127318, -0.024909964, 0.008603549, -0.0041707046, 0.019398315, -0.024022723, -0.013409438, -0.027880875, 0.0023558936, -0.024237812, 0.034172222, -0.006251016, -0.048152987, -0.01523097, -0.002308843, -0.013691742, -0.02688609, 0.007810409, 0.011513968, -0.006647586, -0.011735778, 0.0017408744, -0.17422187, 0.01301959, 0.018860593, -0.00068013405, 0.008791751, -0.031618044, 0.017946465, 0.011735778, -0.03129541, 0.0033607613, 0.0072861305, 0.008227143, -0.018443858, -0.014007653, 0.009961297, 0.006284624, -0.024815861, 0.012676792, 0.014222742, 0.0036632298, 0.0028364826, -0.012320551, -0.0050478633, 0.011729057, 0.023135481, 0.025945077, 0.005676326, -0.007192029, 0.0015308268, -0.019492416, -0.008932903, -0.021737404, 0.012925488, 0.008092714, 0.03245151, -0.009457182, -0.018524516, 0.0025188907, -0.008569942, 0.0022769158, -0.004617686, 0.01315402, 0.024291582, -0.001880346, 0.0014274834, 0.04277577, 0.010216715, -0.018699275, 0.018645504, 0.008059106, 0.02997799, -0.021576088, 0.004846218, 0.015741806, 0.0023542133, 0.03142984, 0.01372535, 0.01598378, 0.001151901, -0.012246614, -0.004184148, -0.023605987, 0.008657321, -0.025770318, -0.019048795, -0.023054823, 0.005535174, -0.018161554, -0.019761277, 0.01385978, -0.016655933, 0.01416897, 0.015311629, 0.008919461, 0.0077499156, 0.023888292, 0.015257857, 0.009087498, 0.0017845642, 0.0013762318, -0.023713533, 0.027464142, -0.014021097, -0.024681432, -0.006741687, 0.0016450927, -0.005804035, -0.002821359, 0.0056796866, -0.023189254, 0.00723908, -0.013483374, -0.018390086, -0.018847149, 0.0061905226, 0.033365637, 0.008489283, 0.015257857, 0.019694062, -0.03019308, -0.012253336, 0.0021744126, -0.00754827, 0.01929077, 0.025044393, 0.017677605, 0.02503095, 0.028579915, 0.01774482, 0.0029961187, -0.019895706, 0.001165344, -0.0075281053, 0.02105181, -0.009221929, 0.023404341, -0.0028079161, -0.0037237236, 0.02847237, 0.0009821824, 0.04629785, -0.017771706, -0.038904175, 0.00869765, 0.0016249281, 0.020984594, -0.10867358, -0.008395182, -0.0010830053, 0.008059106, -0.020097353, 0.0020383017, 0.008038941, -0.009047169, -0.007252523, 0.0286068, -0.0037774958, -0.024923407, 0.005279756, -0.009524398, 0.011527412, -0.0020198175, 0.019452088, 0.014384058, -0.025609002, 0.006025845, -0.030542599, 0.016790364, 0.019223554, -0.012434817, 0.003901844, -0.007817131, -0.027612016, 0.008314524, 0.007938119, -0.0004868903, 0.014747021, -0.009457182, 0.014706692, -0.018847149, 0.015311629, 0.015647706, -0.0031288688, -0.0032717013, 0.008879132, -0.034629285, 0.0090337265, 0.004382433, 0.011305601, -0.028391711, 0.0053268066, 0.0003566608, -0.019169783, 0.011507247, 0.023592545, -0.006603896, -0.009685714, 0.010714107, -0.027907763, 0.006412333, 0.0045706355, -0.029816674, 0.0047958065, 0.0018500991, -0.011500525, 0.0030179636, 0.015997224, -0.022140697, -0.0001849469, -0.014263071, 0.011540854, -0.006607257, -0.01871272, -0.0038480717, -0.0024903242, -0.031214751, -0.0050478633, 0.021481987, -0.012912045, 0.028122852, -0.018605174, -0.00723908, 0.0023609349, -0.0073331813, 0.014935223, -0.005699851, -0.0068895607, -0.015244413, 0.029789789, -0.02458733, 0.0004453009, 0.0015577129, 0.0048596608, 0.009376524, -0.011984475, -0.014518489, 0.015647706, 0.0068794787, 0.0065534846, 0.003107024, -0.01973439, 0.027383484, -0.015459502, -0.006318231, 0.020863606, -0.0021357639, -0.0076692575, -0.021266898, -0.046862457, 0.025326697, 0.016521502, -0.0036833945, 0.0029860365, -0.016306413, 0.026496243, -0.016803807, 0.008724537, -0.0025407355, -0.027302826, 0.017798591, 0.0060796174, -0.014007653, -0.01650806, -0.0095042335, 0.009242094, -0.009342916, 0.010330981, 0.009544563, 0.018591732, 0.0036867552, 0.0194252, 0.0092488155, -0.007823853, 0.0015501512, -0.012031525, 0.010203271, -0.0074272826, -0.020258669, 0.025662774, -0.03032751, 0.014854565, 0.010835094, 0.0007708746, 0.0009989863, -0.014007653, -0.012871716, 0.023444671, 0.03323121, -0.034575514, -0.024291582, 0.011634956, -0.025958521, -0.01973439, 0.0029742739, 0.0067148013, 0.0022399474, 0.011802994, 0.011151006, -0.0116416775, 0.030166194, 0.013039754, -0.022517102, -0.011466918, -0.0033053088, 0.006156915, 0.004829414, 0.006029206, -0.016534945, 0.015325071, -0.0109359175, 0.032854803, -0.001010749, 0.0021155993, -0.011702171, -0.009766373, 0.00679882, 0.0040900465, -0.019438643, -0.006758491, -0.0040060277, 0.022436442, 0.025850976, 0.006150193, 0.018632062, -0.0077230297, -0.015298186, -0.017381858, 0.01911601, -0.005763706, -0.0022281848, -0.031994447, 0.0015972018, 0.028848775, 0.014572261, -0.0073264595, -0.009551284, -0.0052058194, 0.014518489, -0.0041068504, 0.010754436, 0.0055519775, -0.005804035, -0.0054007433, 0.028579915, -0.01791958, -0.015284742, 0.036807057, 0.015069654, -0.0023810994, -0.0038648755, 0.0015467904, -0.0037136413, 0.0023458113, 0.019008467, -0.011547576, -0.010001626, 0.012347437, 0.0155267175, 0.01907568, -0.003041489, -0.0132414, 0.017449073, 0.00060073606, -0.008536334, 0.008233866, -0.0085430555, -0.02365976, 0.024089938, -0.0034615842, -0.006580371, 0.008327967, -0.01509654, 0.009692436, 0.025635887, 0.0020282194, -0.04022159, -0.0021290423, -0.012407931, -0.0021727323, 0.006506434, -0.005320085, -0.008240587, 0.020984594, -0.014491603, 0.003592654, 0.0072121937, -0.03081146, 0.043770555, 0.009302587, -0.003217929, 0.019008467, -0.011271994, 0.02917141, 0.0019576435, -0.0077431942, -0.0030448497, -0.023726976, 0.023377456, -0.006382086, 0.025716545, -0.017341528, 0.0035556855, -0.019129453, -0.004311857, -0.003253217, -0.014935223, 0.0036363439, 0.018121226, -0.0066543072, 0.02458733, 0.0035691285, 0.0039085653, -0.014209299, 0.020191453, 0.0357585, 0.007830574, -0.024130266, -0.008912739, 0.008314524, -0.0346024, -0.0014005973, -0.006788738, -0.021777734, 0.010465411, -0.004012749, -0.00679882, 0.009981461, -0.026227381, 0.027033964, -0.015567047, -0.0063115098, 0.0023071626, 0.01037131, 0.015741806, -0.020635074, -0.012945653));
    private static final int LIMIT = 2;
    private static Map<MongoNamespace, CollectionHelper<BsonDocument>> collectionHelpers;

    @BeforeAll
    static void beforeAll() {
        collectionHelpers = new HashMap<>();
        collectionHelpers.put(MFLIX_MOVIES_NS, new CollectionHelper<>(new BsonDocumentCodec(), MFLIX_MOVIES_NS));
        collectionHelpers.put(MFLIX_EMBEDDED_MOVIES_NS, new CollectionHelper<>(new BsonDocumentCodec(), MFLIX_EMBEDDED_MOVIES_NS));
        collectionHelpers.put(AIRBNB_LISTINGS_AND_REVIEWS_NS, new CollectionHelper<>(new BsonDocumentCodec(), AIRBNB_LISTINGS_AND_REVIEWS_NS));
    }

    @BeforeEach
    void beforeEach() {
        assumeTrue(isAtlasSearchTest());
    }

    private static Stream<Arguments> vectorSearchArgs(){
        return Stream.of(
                arguments(approximateVectorSearchOptions(LIMIT + 1)),
                arguments(exactVectorSearchOptions())
        );
    }

    @ParameterizedTest
    @MethodSource("vectorSearchArgs")
    void vectorSearch(final VectorSearchOptions vectorSearchOptions) {
        assumeTrue(serverVersionAtLeast(7, 1));
        CollectionHelper<BsonDocument> collectionHelper = collectionHelpers.get(MFLIX_EMBEDDED_MOVIES_NS);
        assertAll(
                () -> {
                    List<Bson> pipeline = singletonList(
                            Aggregates.vectorSearch(
                                    // `multi` is used here only to verify that it is tolerated
                                    fieldPath("plot_embedding").multi("ignored"),
                                    QUERY_VECTOR, "sample_mflix__embedded_movies", LIMIT, vectorSearchOptions)
                    );
                    Asserters.size(LIMIT)
                            .accept(collectionHelper.aggregate(pipeline), msgSupplier(pipeline));
                },
                () -> {
                    List<Bson> pipeline = asList(
                            Aggregates.vectorSearch(
                                    fieldPath("plot_embedding"), QUERY_VECTOR, "sample_mflix__embedded_movies", LIMIT,
                                    vectorSearchOptions.filter(gte("year", 2016))),
                            Aggregates.project(
                                    metaVectorSearchScore("vectorSearchScore"))
                    );
                    List<BsonDocument> results = collectionHelper.aggregate(pipeline);
                    Asserters.size(1)
                            .accept(results, msgSupplier(pipeline));
                    Asserters.firstResult((doc, msgSupplier) ->
                            assertTrue(doc.getDouble("vectorSearchScore").doubleValue() > 0, msgSupplier))
                            .accept(results,  msgSupplier(pipeline));
                }
        );
    }

    private static Stream<Arguments> vectorSearchSupportedFiltersArgs(){
        return Stream.of(
                arguments(approximateVectorSearchOptions(1)),
                arguments(exactVectorSearchOptions())
        );
    }

    @ParameterizedTest
    @MethodSource("vectorSearchSupportedFiltersArgs")
    void vectorSearchSupportedFilters(final VectorSearchOptions vectorSearchOptions) {
        assumeTrue(serverVersionAtLeast(7, 1));
        CollectionHelper<BsonDocument> collectionHelper = collectionHelpers.get(MFLIX_EMBEDDED_MOVIES_NS);
        Consumer<Bson> asserter = filter -> {
            List<Bson> pipeline = singletonList(
                    Aggregates.vectorSearch(
                            fieldPath("plot_embedding"), QUERY_VECTOR, "sample_mflix__embedded_movies", 1,
                            vectorSearchOptions.filter(filter))
            );
            Asserters.nonEmpty()
                    .accept(collectionHelper.aggregate(pipeline), msgSupplier(pipeline));
        };
        assertAll(
                () -> asserter.accept(lt("year", 2016)),
                () -> asserter.accept(lte("year", 2016)),
                () -> asserter.accept(eq("year", 2016)),
                () -> asserter.accept(gte("year", 2016)),
                () -> asserter.accept(gt("year", 2015)),
                () -> asserter.accept(ne("year", 2016)),
                () -> asserter.accept(in("year", 2000, 2016)),
                () -> asserter.accept(nin("year", 2000, 2016)),
                () -> asserter.accept(and(gte("year", 2015), lte("year", 2016))),
                () -> asserter.accept(or(eq("year", 2015), eq("year", 2016)))
        );
    }

    /**
     * @param stageUnderTestCreator A {@link CustomizableSearchStageCreator} that is used to create both
     * {@code $search} and {@code $searchMeta} stages. Any combination of an {@link SearchOperator}/{@link SearchCollector} and
     * {@link SearchOptions} that is valid for the {@code $search} stage is also valid for the {@code $searchMeta} stage.
     * This is why we use the same creator for both.
     * @param accessories A list of {@link Accessory} objects that specify additional pipeline stages and an asserter.
     * <ul>
     *  <li>The item with index 0 is used with {@code $search};</li>
     *  <li>the idem with index 1 is used with {@code $searchMeta}.</li>
     * </ul>
     */
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("searchAndSearchMetaArgs")
    void searchAndSearchMeta(
            @SuppressWarnings("unused") final String testDescription,
            final CustomizableSearchStageCreator stageUnderTestCreator,
            final MongoNamespace ns,
            final List<Accessory> accessories) {
        List<BiFunction<Bson, SearchOptions, Bson>> stageUnderTestCustomizers = asList(
                (bsonOperatorOrCollector, options) -> {
                    if (bsonOperatorOrCollector instanceof SearchOperator) {
                        return Aggregates.search((SearchOperator) bsonOperatorOrCollector, options);
                    } else if (bsonOperatorOrCollector instanceof SearchCollector) {
                        return Aggregates.search((SearchCollector) bsonOperatorOrCollector, options);
                    } else {
                        throw Assertions.fail();
                    }
                },
                (bsonOperatorOrCollector, options) -> {
                    if (bsonOperatorOrCollector instanceof SearchOperator) {
                        return Aggregates.searchMeta((SearchOperator) bsonOperatorOrCollector, options);
                    } else if (bsonOperatorOrCollector instanceof SearchCollector) {
                        return Aggregates.searchMeta((SearchCollector) bsonOperatorOrCollector, options);
                    } else {
                        throw Assertions.fail();
                    }
                }
        );
        Assertions.assertTrue(stageUnderTestCustomizers.size() == accessories.size());
        for (int i = 0; i < stageUnderTestCustomizers.size(); i++) {
            Bson stageUnderTest = stageUnderTestCreator.apply(stageUnderTestCustomizers.get(i));
            Accessory accessory = accessories.get(i);
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(stageUnderTest);
            pipeline.addAll(accessory.postStages);
            Supplier<String> msgSupplier = msgSupplier(pipeline);
            List<BsonDocument> results;
            try {
                results = collectionHelpers.get(ns).aggregate(pipeline);
            } catch (Exception e) {
                throw new RuntimeException(msgSupplier.get(), e);
            }
            accessory.resultAsserter.accept(results, msgSupplier);
        }
    }

    /**
     * @see #searchAndSearchMeta(String, CustomizableSearchStageCreator, MongoNamespace, List)
     */
    private static Stream<Arguments> searchAndSearchMetaArgs() {
        return Stream.of(
                arguments(
                        "default options",
                        stageCreator(
                                exists(fieldPath("tomatoes.dvd")),
                                searchOptions()
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        asList(limit(1), project(metaSearchScore("score"))),
                                        Asserters.score(1)
                                ),
                                new Accessory(
                                        emptyList(),
                                        // specifying a bare operator works as if `SearchCount.lowerBound` were specified
                                        Asserters.countLowerBound(1_001)
                                )
                        )
                ),
                arguments(
                        "`index`, `count` options",
                        stageCreator(
                                // `multi` is used here only to verify that it is tolerated
                                exists(fieldPath("title").multi("ignored")),
                                searchOptions()
                                        .option("index", "default")
                                        .count(lowerBound().threshold(2_000))
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        asList(limit(1), project(computedSearchMeta("meta"))),
                                        Asserters.countLowerBound("meta", 2_000)
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countLowerBound(2_000)
                                )
                        )
                ),
                arguments(
                        "`highlight` option",
                        stageCreator(
                                text(singleton(fieldPath("plot")), asList("factory", "century")),
                                searchOptions()
                                        .highlight(paths(
                                                fieldPath("title").multi("keyword"),
                                                wildcardPath("pl*t"))
                                                .maxCharsToExamine(100_000))
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        asList(limit(1), project(metaSearchHighlights("highlights"))),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(1, doc.getArray("highlights").size(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "`returnStoredSource` option",
                        stageCreator(
                                exists(fieldPath("plot")),
                                searchOptions()
                                        .returnStoredSource(true)
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        singleton(limit(1)),
                                        Asserters.firstResult((doc, msgSupplier) -> {
                                            // assert that the fields specified in `storedSource` and "id" were returned
                                            assertNotNull(doc.get("_id"), msgSupplier);
                                            assertFalse(doc.get("plot").asString().getValue().isEmpty(), msgSupplier);
                                            assertEquals(2, doc.size(), msgSupplier);
                                        })
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "alternate analyzer (`multi` field path)",
                        stageCreator(
                                text(singleton(fieldPath("title").multi("keyword")), singleton("Top Gun")),
                                searchOptions().count(total())
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(
                                                "Top Gun", doc.getString("title").getValue(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countTotal(1)
                                )
                        )
                ),
                arguments(
                        "facet collector",
                        stageCreator(
                                facet(
                                        exists(fieldPath("tomatoes")),
                                        asList(
                                                stringFacet(
                                                        "fullplotFacet",
                                                        fieldPath("fullplot"))
                                                        .numBuckets(1),
                                                numberFacet(
                                                        "tomatoesMeterFacet",
                                                        fieldPath("tomatoes.viewer.meter"),
                                                        asList(10f, 20d, 90, Long.MAX_VALUE / 2, Long.MAX_VALUE))
                                                        .defaultBucket("defaultBucket"),
                                                dateFacet(
                                                        "releasedFacet",
                                                        fieldPath("released"),
                                                        asList(
                                                                Instant.EPOCH,
                                                                Instant.from(Year.of(1985)
                                                                        .atMonth(Month.JANUARY).atDay(1).atStartOfDay().atOffset(UTC)),
                                                                Instant.now())))),
                                searchOptions()
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        asList(limit(1), project(computedSearchMeta("meta")), replaceWith("$meta")),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(5, doc.getDocument("facet")
                                                .getDocument("tomatoesMeterFacet").getArray("buckets").size(), msgSupplier))
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.firstResult((doc, msgSupplier) -> assertEquals(5, doc.getDocument("facet")
                                                .getDocument("tomatoesMeterFacet").getArray("buckets").size(), msgSupplier))
                                )
                        )
                ),
                arguments(
                        "score modifier",
                        stageCreator(compound()
                                .should(asList(
                                        exists(fieldPath("fieldName1"))
                                                .score(boost(Float.MAX_VALUE / 2)),
                                        exists(fieldPath("fieldName2"))
                                                .score(boost(fieldPath("boostFieldName"))),
                                        exists(fieldPath("fieldName3"))
                                                .score(boost(fieldPath("boostFieldName"))
                                                        .undefined(-1)),
                                        exists(fieldPath("fieldName4"))
                                                .score(constant(1.2f)),
                                        exists(fieldPath("fieldName5"))
                                                .score(function(relevanceExpression())),
                                        exists(fieldPath("fieldName6"))
                                                .score(function(pathExpression(fieldPath("expressionFieldName")))),
                                        exists(fieldPath("fieldName7"))
                                                .score(function(pathExpression(fieldPath("expressionFieldName"))
                                                        .undefined(-1))),
                                        exists(fieldPath("fieldName8"))
                                                .score(function(constantExpression(-1.2f))),
                                        exists(fieldPath("fieldName9"))
                                                .score(function(
                                                        gaussExpression(-10, pathExpression(fieldPath("gaussianFieldName")), Double.MAX_VALUE / -2))),
                                        exists(fieldPath("fieldName10"))
                                                .score(function(
                                                        gaussExpression(
                                                                -10,
                                                                pathExpression(fieldPath("gaussianFieldName"))
                                                                        .undefined(0),
                                                                Double.MAX_VALUE / -2)
                                                        .offset(Double.MAX_VALUE / -2)
                                                        .decay(Double.MIN_VALUE))),
                                        exists(fieldPath("fieldName11"))
                                                .score(function(logExpression(constantExpression(3)))),
                                        exists(fieldPath("fieldName12"))
                                                .score(function(log1pExpression(constantExpression(-3)))),
                                        exists(fieldPath("fieldName13"))
                                                .score(function(addExpression(asList(
                                                        logExpression(multiplyExpression(asList(
                                                                constantExpression(2),
                                                                constantExpression(3),
                                                                relevanceExpression()))),
                                                        gaussExpression(0, pathExpression(fieldPath("gaussianFieldName")), 1)))))
                                )),
                                searchOptions()
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.empty()
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                )
                        )
                ),
                arguments(
                        "all operators in a `compound` operator",
                        stageCreator(compound()
                                .should(asList(
                                        exists(fieldPath("fieldName1")),
                                        text(fieldPath("fieldName2"), "term1")
                                                .score(function(logExpression(constantExpression(3)))),
                                        text(asList(wildcardPath("wildc*rd"), fieldPath("fieldName3")), asList("term2", "term3"))
                                                .fuzzy(fuzzySearchOptions()
                                                        .maxEdits(1)
                                                        .prefixLength(2)
                                                        .maxExpansions(3)),
                                        autocomplete(fieldPath("title")
                                                // `multi` is used here only to verify that it is tolerated
                                                .multi("ignored"), "term4"),
                                        // this operator produces non-empty search results
                                        autocomplete(fieldPath("title"), "Traffic in", "term5")
                                                .fuzzy()
                                                .sequentialTokenOrder(),
                                        numberRange(fieldPath("fieldName4"), fieldPath("fieldName5"))
                                                .gtLt(1, 1.5),
                                        dateRange(fieldPath("fieldName6"))
                                                .lte(Instant.ofEpochMilli(1)),
                                        near(0, 1.5, fieldPath("fieldName7"), fieldPath("fieldName8")),
                                        near(Instant.ofEpochMilli(1), Duration.ofMillis(3), fieldPath("fieldName9")),
                                        phrase(fieldPath("fieldName10"), "term6"),
                                        in(fieldPath("fieldName10"), true),
                                        in(fieldPath("fieldName11"), "term4", "term5"),
                                        regex(fieldPath("title").multi("keyword"), "term7"),
                                        queryString(fieldPath("fieldName12"), "term8"),
                                        moreLikeThis(new BsonDocument("like", new BsonDocument("fieldName10",
                                                new BsonString("term6")))),
                                        wildcard(asList("term10", "term11"), asList(wildcardPath("wildc*rd"), fieldPath("title").multi(
                                                "keyword"))),
                                        SearchOperator.equals(fieldPath("fieldName11"), "term7"),
                                        equalsNull(fieldPath("fieldName12"))
                                ))
                                .minimumShouldMatch(1)
                                .mustNot(singleton(
                                        compound().must(singleton(exists(fieldPath("fieldName")))))),
                                searchOptions()
                        ),
                        MFLIX_MOVIES_NS,
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countLowerBound(0)
                                )
                        )
                ),
                arguments(
                        "geo operators in a `compound` operator",
                        stageCreator(compound()
                                .should(singleton(
                                        near(
                                                new Point(new Position(114.15, 22.28)),
                                                1234.5,
                                                fieldPath("address.location"))
                                )),
                                searchOptions()
                        ),
                        AIRBNB_LISTINGS_AND_REVIEWS_NS,
                        asList(
                                new Accessory(
                                        emptyList(),
                                        Asserters.nonEmpty()
                                ),
                                new Accessory(
                                        emptyList(),
                                        Asserters.countLowerBound(0)
                                )
                        )
                )
        );
    }

    private static Supplier<String> msgSupplier(final Collection<? extends Bson> pipeline) {
        return () -> "For reference, the pipeline (" + pipeline.size() + " elements) used in the test is\n[\n"
                + pipeline.stream()
                .map(stage -> stage.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry()))
                .map(doc -> doc.toJson(JsonWriterSettings.builder().indent(true).build()))
                .collect(Collectors.joining(",\n"))
                + "\n]\n";
    }

    private static final class Asserters {
        static Asserter empty() {
            return decorate((results, msgSupplier) -> assertTrue(results.isEmpty(), msgSupplier));
        }

        static Asserter nonEmpty() {
            return decorate((results, msgSupplier) -> assertFalse(results.isEmpty(), msgSupplier));
        }

        static Asserter size(final int expectedSize) {
            return decorate((results, msgSupplier) -> assertEquals(expectedSize, results.size(), msgSupplier));
        }

        /**
         * Checks the value of the {@code "score"} field for each result document.
         */
        static Asserter score(final double expectedScore) {
            return decorate((results, msgSupplier) -> {
                assertFalse(results.isEmpty(), msgSupplier);
                for (BsonDocument result : results) {
                    assertEquals(expectedScore, result.getNumber("score").doubleValue(), 0.000_1, msgSupplier);
                }
            });
        }

        /**
         * Checks the value of the {@code "customMetaField.count.lowerBound"} field.
         */
        static Asserter countLowerBound(final String customMetaField, final int expectedAtLeast) {
            return firstResult((doc, msgSupplier) -> assertTrue(
                    doc.getDocument(customMetaField).getDocument("count").getNumber("lowerBound").intValue() >= expectedAtLeast, msgSupplier));
        }

        /**
         * Checks the value of the {@code "count.lowerBound"} field.
         */
        static Asserter countLowerBound(final int expectedAtLeast) {
            return firstResult((doc, msgSupplier) -> assertTrue(
                    doc.getDocument("count").getNumber("lowerBound").intValue() >= expectedAtLeast, msgSupplier));
        }

        /**
         * Checks the value of the {@code "count.total"} field.
         */
        static Asserter countTotal(final int expected) {
            return firstResult((doc, msgSupplier) -> assertEquals(
                    expected, doc.getDocument("count").getNumber("total").intValue(), msgSupplier));
        }

        static Asserter firstResult(final BiConsumer<BsonDocument, Supplier<String>> asserter) {
            return decorate((results, msgSupplier) -> {
                assertFalse(results.isEmpty(), msgSupplier);
                asserter.accept(results.get(0), msgSupplier);
            });
        }

        private static Asserter decorate(final Asserter asserter) {
            int maxRenderedResults = 20;
            return (results, msgSupplier) -> asserter.accept(
                    results,
                    () -> msgSupplier.get()
                            + "\ntop " + maxRenderedResults + " out of total " + results.size() + " results are\n["
                            + results.stream()
                            .map(doc -> doc.toJson(JsonWriterSettings.builder().indent(true).build()))
                            .limit(maxRenderedResults)
                            .collect(Collectors.joining(",\n"))
                            + "\n]\n"
            );
        }
    }

    private static CustomizableSearchStageCreator stageCreator(final Bson operatorOrCollector, final SearchOptions options) {
        return customizer -> customizer.apply(operatorOrCollector, options);
    }

    @FunctionalInterface
    private interface CustomizableSearchStageCreator extends Function<BiFunction<Bson, SearchOptions, Bson>, Bson> {
    }

    @FunctionalInterface
    private interface Asserter extends BiConsumer<List<BsonDocument>, Supplier<String>> {
    }

    private static final class Accessory {
        private final Collection<Bson> postStages;
        private final BiConsumer<List<BsonDocument>, Supplier<String>> resultAsserter;

        Accessory(
                final Collection<Bson> postStages,
                final BiConsumer<List<BsonDocument>, Supplier<String>> resultAsserter) {
            this.postStages = postStages;
            this.resultAsserter = resultAsserter;
        }
    }
}
