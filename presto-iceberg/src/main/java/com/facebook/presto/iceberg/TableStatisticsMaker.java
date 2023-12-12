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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.iceberg.stats.DistinctValuesSerde;
import com.facebook.presto.iceberg.stats.IcebergStatsSerde;
import com.facebook.presto.iceberg.stats.TDigestHistogramSerde;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TypeUtils.isNumericType;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.iceberg.ExpressionConverter.toIcebergExpression;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static com.facebook.presto.iceberg.IcebergSessionProperties.getStatisticSnapshotRecordDifferenceWeight;
import static com.facebook.presto.iceberg.IcebergUtil.getIdentityPartitions;
import static com.facebook.presto.iceberg.Partition.toMap;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.HISTOGRAM;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Long.parseLong;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;

public class TableStatisticsMaker
{
    private static final Logger log = Logger.get(TableStatisticsMaker.class);

    private static final Set<IcebergStatsSerde> serdes = new ImmutableSet.Builder<IcebergStatsSerde>()
            .add(new DistinctValuesSerde())
            .add(new TDigestHistogramSerde())
            .build();

    private static final Map<String, IcebergStatsSerde> blobTypeToSerde = serdes.stream().collect(toImmutableMap(IcebergStatsSerde::blobType, identity()));

    private static final Map<ColumnStatisticType, IcebergStatsSerde> statTypeToSerde = serdes.stream().collect(toImmutableMap(IcebergStatsSerde::statisticType, identity()));

    private final Table icebergTable;
    private final TypeManager typeManager;
    private final ConnectorSession session;

    private TableStatisticsMaker(Table icebergTable, ConnectorSession session, TypeManager typeManager)
    {
        this.icebergTable = icebergTable;
        this.session = session;
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Table getIcebergTable()
    {
        return icebergTable;
    }

    public TypeManager getTypeManager()
    {
        return typeManager;
    }

    public ConnectorSession getSession()
    {
        return session;
    }

    public static TableStatistics getTableStatistics(TypeManager typeManager, ConnectorSession session, Constraint constraint, IcebergTableHandle tableHandle, Table icebergTable, List<IcebergColumnHandle> columns)
    {
        return new TableStatisticsMaker(icebergTable, session, typeManager).makeTableStatistics(tableHandle, constraint, columns);
    }

    private TableStatistics makeTableStatistics(IcebergTableHandle tableHandle, Constraint constraint, List<IcebergColumnHandle> selectedColumns)
    {
        if (!tableHandle.getIcebergTableName().getSnapshotId().isPresent() || constraint.getSummary().isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        TupleDomain<IcebergColumnHandle> intersection = constraint.getSummary()
                .transform(IcebergColumnHandle.class::cast)
                .intersect(tableHandle.getPredicate());

        if (intersection.isNone()) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        List<Types.NestedField> columns = icebergTable.schema().columns();

        Map<Integer, Type.PrimitiveType> idToTypeMapping = columns.stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, column -> column.type().asPrimitiveType()));
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toSet());

        List<Types.NestedField> nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        TableScan tableScan = icebergTable.newScan()
                .filter(toIcebergExpression(intersection))
                .select(selectedColumns.stream().map(IcebergColumnHandle::getName).collect(Collectors.toList()))
                .useSnapshot(tableHandle.getIcebergTableName().getSnapshotId().get())
                .includeColumnStats();

        Partition summary = null;
        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {
                DataFile dataFile = fileScanTask.file();

                if (summary == null) {
                    summary = new Partition(
                            idToTypeMapping,
                            nonPartitionPrimitiveColumns,
                            dataFile.partition(),
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(idToTypeMapping, dataFile.lowerBounds()),
                            toMap(idToTypeMapping, dataFile.upperBounds()),
                            dataFile.nullValueCounts(),
                            dataFile.columnSizes());
                }
                else {
                    summary.incrementFileCount();
                    summary.incrementRecordCount(dataFile.recordCount());
                    summary.incrementSize(dataFile.fileSizeInBytes());
                    updateSummaryMin(summary, partitionFields, toMap(idToTypeMapping, dataFile.lowerBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    updateSummaryMax(summary, partitionFields, toMap(idToTypeMapping, dataFile.upperBounds()), dataFile.nullValueCounts(), dataFile.recordCount());
                    summary.updateNullCount(dataFile.nullValueCounts());
                    updateColumnSizes(summary, dataFile.columnSizes());
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (summary == null) {
            return TableStatistics.builder()
                    .setRowCount(Estimate.of(0))
                    .build();
        }

        double recordCount = summary.getRecordCount();
        TableStatistics.Builder result = TableStatistics.builder();
        result.setRowCount(Estimate.of(recordCount));
        result.setTotalSize(Estimate.of(summary.getSize()));
        Map<Integer, ColumnStatistics.Builder> tableStats = getClosestStatisticsFileForSnapshot(tableHandle)
                .map(this::loadStatisticsFile).orElseGet(Collections::emptyMap);
        for (IcebergColumnHandle columnHandle : selectedColumns) {
            int fieldId = columnHandle.getId();
            ColumnStatistics.Builder columnBuilder = tableStats.getOrDefault(fieldId, ColumnStatistics.builder());
            Long nullCount = summary.getNullCounts().get(fieldId);
            if (nullCount != null) {
                columnBuilder.setNullsFraction(Estimate.of(nullCount / recordCount));
            }
            if (summary.getColumnSizes() != null) {
                Long columnSize = summary.getColumnSizes().get(fieldId);
                if (columnSize != null) {
                    columnBuilder.setDataSize(Estimate.of(columnSize));
                }
            }
            Object min = summary.getMinValues().get(fieldId);
            Object max = summary.getMaxValues().get(fieldId);
            if (min instanceof Number && max instanceof Number) {
                columnBuilder.setRange(Optional.of(new DoubleRange(((Number) min).doubleValue(), ((Number) max).doubleValue())));
            }
            result.setColumnStatistics(columnHandle, columnBuilder.build());
        }
        return result.build();
    }

    public static void writeTableStatistics(TypeManager typeManager, NodeVersion nodeVersion, IcebergTableHandle tableHandle, Table icebergTable, ConnectorSession session, Collection<ComputedStatistics> computedStatistics)
    {
        new TableStatisticsMaker(icebergTable, session, typeManager).writeTableStatistics(nodeVersion, tableHandle, computedStatistics);
    }

    private void writeTableStatistics(NodeVersion nodeVersion, IcebergTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        Snapshot snapshot = tableHandle.getIcebergTableName().getSnapshotId().map(icebergTable::snapshot).orElseGet(icebergTable::currentSnapshot);
        if (snapshot == null) {
            // this may occur if the table has not been written to.
            return;
        }
        try (FileIO io = icebergTable.io()) {
            String path = ((HasTableOperations) icebergTable).operations().metadataFileLocation(format("%s-%s.stats", session.getQueryId(), randomUUID()));
            OutputFile outputFile = io.newOutputFile(path);
            try (PuffinWriter writer = Puffin.write(outputFile)
                    .createdBy("presto-" + nodeVersion)
                    .build()) {
                computedStatistics.stream()
                        .map(ComputedStatistics::getColumnStatistics)
                        .filter(Objects::nonNull)
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                        .forEach((key, value) -> Optional.ofNullable(statTypeToSerde.get(key.getStatisticType()))
                                .map(serde -> serde.serialize(key, value, this, snapshot))
                                .ifPresent(writer::add));
                writer.finish();
                icebergTable.updateStatistics().setStatistics(
                                snapshot.snapshotId(),
                                new GenericStatisticsFile(
                                        snapshot.snapshotId(),
                                        path,
                                        writer.fileSize(),
                                        writer.footerSize(),
                                        writer.writtenBlobsMetadata().stream()
                                                .map(GenericBlobMetadata::from)
                                                .collect(toImmutableList())))
                        .commit();
            }
            catch (IOException e) {
                log.warn(e, "failed to write table statistics file");
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "failed to write statistics file", e);
            }
        }
    }

    public void updateColumnSizes(Partition summary, Map<Integer, Long> addedColumnSizes)
    {
        Map<Integer, Long> columnSizes = summary.getColumnSizes();
        if (!summary.hasValidColumnMetrics() || columnSizes == null || addedColumnSizes == null) {
            return;
        }
        for (Types.NestedField column : summary.getNonPartitionPrimitiveColumns()) {
            int id = column.fieldId();

            Long addedSize = addedColumnSizes.get(id);
            if (addedSize != null) {
                columnSizes.put(id, addedSize + columnSizes.getOrDefault(id, 0L));
            }
        }
    }

    private void updateSummaryMin(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> lowerBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMinValues(), lowerBounds, nullCounts, recordCount, i -> (i > 0));
        updatePartitionedStats(summary, partitionFields, summary.getMinValues(), lowerBounds, i -> (i > 0));
    }

    private void updateSummaryMax(Partition summary, List<PartitionField> partitionFields, Map<Integer, Object> upperBounds, Map<Integer, Long> nullCounts, long recordCount)
    {
        summary.updateStats(summary.getMaxValues(), upperBounds, nullCounts, recordCount, i -> (i < 0));
        updatePartitionedStats(summary, partitionFields, summary.getMaxValues(), upperBounds, i -> (i < 0));
    }

    private void updatePartitionedStats(
            Partition summary,
            List<PartitionField> partitionFields,
            Map<Integer, Object> current,
            Map<Integer, Object> newStats,
            Predicate<Integer> predicate)
    {
        for (PartitionField field : partitionFields) {
            int id = field.sourceId();
            if (summary.getCorruptedStats().contains(id)) {
                continue;
            }

            Object newValue = newStats.get(id);
            if (newValue == null) {
                continue;
            }

            Object oldValue = current.putIfAbsent(id, newValue);
            if (oldValue != null) {
                Comparator<Object> comparator = Comparators.forType(summary.getIdToTypeMapping().get(id));
                if (predicate.test(comparator.compare(oldValue, newValue))) {
                    current.put(id, newValue);
                }
            }
        }
    }

    private Optional<StatisticsFile> getClosestStatisticsFileForSnapshot(IcebergTableHandle handle)
    {
        Snapshot target = handle.getIcebergTableName().getSnapshotId().map(icebergTable::snapshot).orElseGet(icebergTable::currentSnapshot);
        return icebergTable.statisticsFiles()
                .stream()
                .min((first, second) -> {
                    if (first == second) {
                        return 0;
                    }
                    if (icebergTable.snapshot(first.snapshotId()) == null) {
                        return 1;
                    }
                    if (icebergTable.snapshot(second.snapshotId()) == null) {
                        return -1;
                    }
                    Snapshot firstSnap = icebergTable.snapshot(first.snapshotId());
                    Snapshot secondSnap = icebergTable.snapshot(second.snapshotId());
                    long firstDiff = abs(target.timestampMillis() - firstSnap.timestampMillis());
                    long secondDiff = abs(target.timestampMillis() - secondSnap.timestampMillis());

                    // check if total-record exists
                    Optional<Long> targetTotalRecords = Optional.ofNullable(target.summary().get(TOTAL_RECORDS_PROP)).map(Long::parseLong);
                    Optional<Long> firstTotalRecords = Optional.ofNullable(firstSnap.summary().get(TOTAL_RECORDS_PROP))
                            .map(Long::parseLong);
                    Optional<Long> secondTotalRecords = Optional.ofNullable(secondSnap.summary().get(TOTAL_RECORDS_PROP))
                            .map(Long::parseLong);

                    if (targetTotalRecords.isPresent() && firstTotalRecords.isPresent() && secondTotalRecords.isPresent()) {
                        long targetTotal = targetTotalRecords.get();
                        double weight = getStatisticSnapshotRecordDifferenceWeight(session);
                        firstDiff += (long) (weight * abs(firstTotalRecords.get() - targetTotal));
                        secondDiff += (long) (weight * abs(secondTotalRecords.get() - targetTotal));
                    }

                    return Long.compare(firstDiff, secondDiff);
                });
    }

    /**
     * Builds a map of field ID to ColumnStatistics for a particular {@link StatisticsFile}.
     *
     * @return a map of field IDs to column statistics stored in the file
     */
    private Map<Integer, ColumnStatistics.Builder> loadStatisticsFile(StatisticsFile file)
    {
        Map<Integer, ColumnStatistics.Builder> result = new HashMap<>();
        try (FileIO io = icebergTable.io()) {
            InputFile input = io.newInputFile(file.path());
            try (PuffinReader reader = Puffin.read(input).withFileSize(file.fileSizeInBytes()).withFooterSize(file.fileFooterSizeInBytes()).build()) {
                reader.readAll(reader.fileMetadata().blobs()).forEach(blob -> {
                    BlobMetadata metadata = blob.first();
                    ByteBuffer data = blob.second();
                    Integer field = getOnlyElement(metadata.inputFields());
                    Optional.ofNullable(blobTypeToSerde.get(metadata.type()))
                            .ifPresent(serde -> {
                                ColumnStatistics.Builder stats = serde.deserialize(metadata, data);
                                result.merge(field, stats, ColumnStatistics.Builder::mergeWith);
                            });
                });
            }
            catch (IOException e) {
                throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, "failed to read statistics file");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public static List<ColumnStatisticMetadata> getSupportedColumnStatistics(String columnName, com.facebook.presto.common.type.Type type)
    {
        ImmutableList.Builder<ColumnStatisticMetadata> supportedStatistics = ImmutableList.builder();
        // all types which support being passed to the sketch_theta function
        if (isNumericType(type) || type.equals(DATE) || isVarcharType(type) ||
                type.equals(TIMESTAMP) ||
                type.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            supportedStatistics.add(NUMBER_OF_DISTINCT_VALUES.getColumnStatisticMetadataWithCustomFunction(columnName, "sketch_theta"));
        }

        if (DOUBLE.equals(type)) {
            supportedStatistics.add(HISTOGRAM.getColumnStatisticMetadata(columnName));
        }

        return supportedStatistics.build();
    }
}
