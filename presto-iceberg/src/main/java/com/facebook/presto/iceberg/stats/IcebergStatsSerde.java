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
import com.facebook.presto.iceberg.TableStatisticsMaker;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;

import java.nio.ByteBuffer;

public interface IcebergStatsSerde
{
    ColumnStatisticType statisticType();

    String blobType();

    Blob serialize(ColumnStatisticMetadata columnStatisticMetadata,
            Block block,
            TableStatisticsMaker tableStatisticsMaker,
            Snapshot snapshot);

    ColumnStatistics.Builder deserialize(BlobMetadata blobMetadata, ByteBuffer data);
}
