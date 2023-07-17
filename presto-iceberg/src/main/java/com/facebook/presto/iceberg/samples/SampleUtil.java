/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg.samples;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.iceberg.IcebergHiveMetadata;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Preconditions;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.getHiveIcebergTable;
import static com.facebook.presto.iceberg.util.IcebergPrestoModelConverters.toIcebergTableIdentifier;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class SampleUtil
{
    private SampleUtil() {}

    public static final TableIdentifier SAMPLE_TABLE_ID = toIcebergTableIdentifier("sample", "sample-table");

    public static Table getNativeTable(ConnectorSession session, IcebergResourceFactory resourceFactory, SchemaTableName table)
    {
        return resourceFactory.getCatalog(session).loadTable(toIcebergTableIdentifier(table.getSchemaName(), table.getTableName()));
    }

    public static Table getHiveTable(ConnectorSession session, IcebergHiveMetadata metadata, HdfsEnvironment env, SchemaTableName table)
    {
        ExtendedHiveMetastore metastore = metadata.getMetastore();
        return getHiveIcebergTable(metastore, env, session, table);
    }

    public static AutoCloseableCatalog getCatalogForSampleTable(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        Path tableLocation = new Path(icebergSource.location());
        HdfsContext ctx = new HdfsContext(session, prestoSchema, icebergSource.name(), icebergSource.location(), false);
        AutoCloseableCatalog c = new AutoCloseableCatalog();
        Map<String, String> props = new HashMap<>();
        c.setConf(env.getConfiguration(ctx, tableLocation));
        props.put(WAREHOUSE_LOCATION, tableLocation.toString());
        // initializing the catalog can be expensive. See if we can somehow store the catalog reference
        // or wrap a delegate catalog.
        c.initialize(tableLocation.getName(), props);
        return c;
    }

    public static boolean sampleTableExists(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        try (AutoCloseableCatalog c = getCatalogForSampleTable(icebergSource, prestoSchema, env, session)) {
            return c.tableExists(SAMPLE_TABLE_ID);
        }
    }

    public static Table getSampleTableFromActual(Table icebergSource, String prestoSchema, HdfsEnvironment env, ConnectorSession session)
    {
        try (AutoCloseableCatalog c = getCatalogForSampleTable(icebergSource, prestoSchema, env, session)) {
            return c.loadTable(SAMPLE_TABLE_ID);
        }
    }

    /**
     * Standard normal distribution used to calculate scores which are used to determine sample
     * sizes.
     */
    private static final NormalDistribution NORMAL_DISTRIBUTION = new NormalDistribution();

    /**
     * Roughly 1 quintillion.
     */
    private static final long MAX_POPULATION_SIZE = 1_000_000_000_000_000L;

    /**
     * Calculates the size of the sample required to achieve a mean within the sample
     * at the given confidence and margin of error for the provided population size.
     * <br>
     * If the population size becomes greater than roughly 1 sextillion, the ability for us to
     * recompute the original population size from the sample size and input parameters decreases.
     * In that case, we will assume the population size to be infinite for the calculations
     *
     * @param confidence a value between (0, 1.0)
     * @param error a value between (0, 1.0)
     * @param populationSize a value > 0.
     * @return the sample size
     */
    public static long calculateSampleSizeFromConfidence(double confidence, double error, long populationSize)
    {
        Preconditions.checkArgument(confidence > 0.0 && confidence < 1.0, "confidence must be in the range (0.0, 1.0)");
        Preconditions.checkArgument(error > 0.0 && error < 1.0, "error must be in the range (0.0, 1.0)");
        Preconditions.checkArgument(populationSize > 0, "population size must be > 0");
        double m = calculateMValue(confidence, error);
        if (populationSize > MAX_POPULATION_SIZE) {
            return (long) Math.ceil(m);
        }
        double sampleSize = m / (1 + ((m - 1) / populationSize));
        return (long) Math.ceil(sampleSize);
    }

    /**
     * With a given sample size, and original confidence and error bounds used to create the sample,
     * reverse the calculation to determine the original population size.
     *
     * @param confidence the confidence interval used to generate the sample
     * @param error the margin of error used to generate the sample
     * @param sampleSize the size of the sample
     * @return the original population size of the sample
     */
    public static long calculatePopulationSizeFromSample(double confidence, double error, long sampleSize)
    {
        Preconditions.checkArgument(confidence > 0.0 && confidence < 1.0, "confidence must be in the range (0.0, 1.0)");
        Preconditions.checkArgument(error > 0.0 && error < 1.0, "error must be in the range (0.0, 1.0)");
        Preconditions.checkArgument(sampleSize > 0, "population size must be > 0");
        double m = calculateMValue(confidence, error);
        return (long)((sampleSize * (m - 1)) / (m - sampleSize));
    }

    public static double calculateMValue(double confidence, double error) {
        double zScore = NORMAL_DISTRIBUTION.inverseCumulativeProbability(confidence + ((1.0 - confidence) / 2));
        return Math.pow(zScore, 2) / (4 * Math.pow(error, 2));
    }

    public static class AutoCloseableCatalog
            extends HadoopCatalog
            implements AutoCloseable
    {
        @Override
        public void close()
        {
            try {
                super.close();
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
            }
        }
    }
}
