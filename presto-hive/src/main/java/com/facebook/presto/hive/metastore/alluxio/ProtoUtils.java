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
package com.facebook.presto.hive.metastore.alluxio;

import alluxio.grpc.table.Layout;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.shaded.client.com.google.protobuf.InvalidProtocolBufferException;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.metastore.SortingColumn;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ProtoUtils
{
    private ProtoUtils() {}

    public static Database fromProto(alluxio.grpc.table.Database db)
    {
        Database database = new Database();
        database.setName(db.getDbName());
        database.setLocationUri(db.getLocation());
        return database;
    }

    public static Table fromProto(alluxio.grpc.table.TableInfo table)
    {
        if (!table.hasLayout()) {
            throw new UnsupportedOperationException("Unsupported table metadata. missing layout.");
        }
        Layout layout = table.getLayout();
        if (!alluxio.table.ProtoUtils.isHiveLayout(layout)) {
            throw new UnsupportedOperationException("Unsupported table layout: " + layout);
        }
        try {
            Table tbl = new org.apache.hadoop.hive.metastore.api.Table();

            PartitionInfo partitionInfo = alluxio.table.ProtoUtils.toHiveLayout(layout);

            // compute the data columns
            tbl.setDbName(table.getDbName());
            tbl.setTableName(table.getTableName());
            tbl.setOwner(table.getOwner());
            tbl.setTableType(table.getType().toString());

            tbl.setParameters(table.getParametersMap());

            alluxio.grpc.table.layout.hive.Storage storage = partitionInfo.getStorage();
            StorageDescriptor sd = fromProto(storage);
            // Set columns
            sd.setCols(table.getSchema().getColsList().stream().map(ProtoUtils::fromProto)
                    .collect(Collectors.toList()));
            tbl.setSd(sd);
            return tbl;
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo from TableInfo", e);
        }
    }

    private static SortingColumn fromProto(alluxio.grpc.table.layout.hive.SortingColumn column)
    {
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.ASCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.ASCENDING);
        }
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.DESCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.DESCENDING);
        }
        throw new IllegalArgumentException("Invalid sort order: " + column.getOrder());
    }

    private static Optional<HiveBucketProperty> fromProto(alluxio.grpc.table.layout.hive.HiveBucketProperty property)
    {
        // must return empty if buckets <= 0
        if (!property.hasBucketCount() || property.getBucketCount() <= 0) {
            return Optional.empty();
        }
        List<SortingColumn> sortedBy = property.getSortedByList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList());
        return Optional.of(new HiveBucketProperty(property.getBucketedByList(), (int) property.getBucketCount(), sortedBy));
    }

    private static org.apache.hadoop.hive.metastore.api.FieldSchema fromProto(alluxio.grpc.table.FieldSchema column)
    {
        org.apache.hadoop.hive.metastore.api.FieldSchema fs =
                new org.apache.hadoop.hive.metastore.api.FieldSchema();
        if (column.hasComment()) {
            fs.setComment(column.getComment());
        }
        fs.setName(column.getName());
        fs.setType(column.getType());
        return fs;
    }

    public static Partition fromProto(alluxio.grpc.table.layout.hive.PartitionInfo info)
    {
        if (!info.hasStorage()) {
            throw new IllegalArgumentException("PartitionInfo must contain storage information");
        }
        Partition part = new Partition();
        part.setDbName(info.getDbName());
        part.setTableName(info.getTableName());
        part.setParameters(info.getParametersMap());
        part.setValues(info.getValuesList());
        StorageDescriptor sd = fromProto(info.getStorage());
        sd.setCols(info.getDataColsList().stream().map(ProtoUtils::fromProto).collect(Collectors.toList()));
        part.setSd(sd);

        return part;
    }

    public static StorageDescriptor fromProto(Storage storage)
    {
        StorageDescriptor sd = new StorageDescriptor();
        // Set columns
        sd.setLocation(storage.getLocation());
        sd.setInputFormat(storage.getStorageFormat().getInputFormat());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormat());
        if (storage.hasBucketProperty()) {
            sd.setBucketCols(storage.getBucketProperty().getBucketedByList());
            sd.setNumBuckets(storage.getBucketProperty().getBucketedByCount());
        }
        SerDeInfo si = new SerDeInfo();
        si.setName(storage.getStorageFormat().getSerde());
        si.setParameters(storage.getStorageFormat().getSerdelibParametersMap());
        sd.setSerdeInfo(si);
        SkewedInfo skewInfo = new SkewedInfo();
        sd.setSkewedInfo(skewInfo);
        return sd;
    }

    public static alluxio.grpc.table.layout.hive.PartitionInfo toPartitionInfo(alluxio.grpc.table.Partition part)
    {
        try {
            return alluxio.table.ProtoUtils.extractHiveLayout(part);
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo", e);
        }
    }

    public static List<alluxio.grpc.table.layout.hive.PartitionInfo> toPartitionInfoList(List<alluxio.grpc.table.Partition> parts)
    {
        return parts.stream().map(ProtoUtils::toPartitionInfo).collect(Collectors.toList());
    }
}
