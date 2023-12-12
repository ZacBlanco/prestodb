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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.cost.TDigestHistogram;
import com.facebook.presto.iceberg.TableStatisticsMaker;
import com.facebook.presto.iceberg.TypeConverter;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.util.Optional;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.HISTOGRAM;
import static java.lang.String.format;

public class TDigestHistogramSerde
        implements IcebergStatsSerde
{
    public static final String BLOB_TYPE = "presto-histogram-tdigest-v1";

    @Override
    public ColumnStatisticType statisticType()
    {
        return HISTOGRAM;
    }

    @Override
    public String blobType()
    {
        return BLOB_TYPE;
    }

    @Override
    public Blob serialize(ColumnStatisticMetadata columnStatisticMetadata, Block block, TableStatisticsMaker tableStatisticsMaker, Snapshot snapshot)
    {
        Schema schema = tableStatisticsMaker.getIcebergTable().schemas().get(snapshot.schemaId());
        if (schema == null) {
            throw new PrestoException(ICEBERG_INVALID_METADATA, format("Couldn't find schema with id %d on snapshot %d", snapshot.schemaId(), snapshot.snapshotId()));
        }
        Types.NestedField field = schema.findField(columnStatisticMetadata.getColumnName());
        TypeConverter.toPrestoType(field.type(), tableStatisticsMaker.getTypeManager());
        ByteBuffer rawDigest = VARBINARY.getSlice(block, 0).toByteBuffer();

        return new Blob(
                BLOB_TYPE,
                ImmutableList.of(field.fieldId()),
                snapshot.snapshotId(),
                snapshot.sequenceNumber(),
                rawDigest,
                null,
                ImmutableMap.of());
    }

    @Override
    public ColumnStatistics.Builder deserialize(BlobMetadata blobMetadata, ByteBuffer data)
    {
        return ColumnStatistics.builder()
                .setHistogram(Optional.of(new TDigestHistogram(Slices.wrappedBuffer(data))));
    }
}
