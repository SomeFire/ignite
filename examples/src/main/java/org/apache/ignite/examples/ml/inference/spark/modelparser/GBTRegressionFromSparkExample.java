/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.ml.inference.spark.modelparser;

import java.io.FileNotFoundException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.tutorial.TitanicUtils;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.sparkmodelparser.SparkModelParser;
import org.apache.ignite.ml.sparkmodelparser.SupportedSparkModels;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Run GBT Regression model loaded from snappy.parquet file. The snappy.parquet file was generated by Spark MLLib
 * model.write.overwrite().save(..) operator.
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class GBTRegressionFromSparkExample {
    /**
     * Path to Spark GBT Regression model.
     */
    public static final String SPARK_MDL_PATH = "examples/src/main/resources/models/spark/serialized/gbtreg";

    /** Learning environment. */
    public static final LearningEnvironment env = LearningEnvironmentBuilder.defaultBuilder().withParallelismStrategyTypeDependency(ParallelismStrategy.ON_DEFAULT_POOL)
        .withLoggingFactoryDependency(ConsoleLogger.Factory.HIGH).buildForTrainer();

    /**
     * Run example.
     */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> GBT Regression model loaded from Spark through serialization over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = TitanicUtils.readPassengersWithoutNulls(ignite);

                final Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>(0, 1, 5, 6).labeled(4);

                ModelsComposition mdl = (ModelsComposition)SparkModelParser.parse(
                    SPARK_MDL_PATH,
                    SupportedSparkModels.GRADIENT_BOOSTED_TREES_REGRESSION,
                    env
                );

                System.out.println(">>> GBT Regression model: " + mdl);

                System.out.println(">>> ---------------------------------");
                System.out.println(">>> | Prediction\t| Ground Truth\t|");
                System.out.println(">>> ---------------------------------");

                try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, Vector> observation : observations) {
                        LabeledVector<Double> lv = vectorizer.apply(observation.getKey(), observation.getValue());
                        Vector inputs = lv.features();
                        double groundTruth = lv.label();
                        double prediction = mdl.predict(inputs);

                        System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                    }
                }

                System.out.println(">>> ---------------------------------");
            }
            finally {
                dataCache.destroy();
            }
        }
    }
}
