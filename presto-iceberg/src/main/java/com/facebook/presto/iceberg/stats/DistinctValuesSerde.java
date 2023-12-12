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
package com.facebook.presto.iceberg.stats;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.iceberg.TableStatisticsMaker;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static java.lang.String.format;

public class DistinctValuesSerde
        implements IcebergStatsSerde
{
    private static final Logger log = Logger.get(DistinctValuesSerde.class);
    public static final String BLOB_TYPE = "apache-datasketches-theta-v1";
    public static final String ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY = "ndv";

    @Override
    public ColumnStatisticType statisticType()
    {
        return NUMBER_OF_DISTINCT_VALUES;
    }

    @Override
    public String blobType()
    {
        return BLOB_TYPE;
    }

    @Override
    public Blob serialize(ColumnStatisticMetadata columnStatisticMetadata, Block block, TableStatisticsMaker tableStatisticsMaker, Snapshot snapshot)
    {
        Table icebergTable = tableStatisticsMaker.getIcebergTable();
        Optional<Integer> id = Optional.ofNullable(icebergTable.schema().findField(columnStatisticMetadata.getColumnName()))
                .map(Types.NestedField::fieldId);
        if (!id.isPresent()) {
            log.warn("failed to find column name %s in schema of table %s when writing distinct value statistics", columnStatisticMetadata.getColumnName(), icebergTable.name());
            throw new PrestoException(ICEBERG_INVALID_METADATA, format("failed to find column name %s in schema of table %s when writing distinct value statistics", columnStatisticMetadata.getColumnName(), icebergTable.name()));
        }
        ByteBuffer raw = VARBINARY.getSlice(block, 0).toByteBuffer();
        CompactSketch sketch = CompactSketch.wrap(Memory.wrap(raw, ByteOrder.nativeOrder()));
        return new Blob(
                BLOB_TYPE,
                ImmutableList.of(id.get()),
                snapshot.snapshotId(),
                snapshot.sequenceNumber(),
                raw,
                null,
                ImmutableMap.of(ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY, Long.toString((long) sketch.getEstimate())));
    }

    @Override
    public ColumnStatistics.Builder deserialize(BlobMetadata blobMetadata, ByteBuffer data)
    {
        ColumnStatistics.Builder colStats = ColumnStatistics.builder();
        Optional.ofNullable(blobMetadata.properties().get(ICEBERG_THETA_SKETCH_BLOB_PROPERTY_NDV_KEY))
                .ifPresent(ndvProp -> {
                    try {
                        long ndv = Long.parseLong(ndvProp);
                        colStats.setDistinctValuesCount(Estimate.of(ndv));
                    }
                    catch (NumberFormatException e) {
                        colStats.setDistinctValuesCount(Estimate.unknown());
                        log.warn("bad long value when parsing statistics blob %s @ %d, bad value: %d", blobMetadata.type(), blobMetadata.snapshotId(), ndvProp);
                    }
                });
        return colStats;
    }
}
