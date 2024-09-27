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
package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorPartitioningMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class TransactionalMemoryMetadata
        implements ConnectorMetadata
{
    private final MemoryMetadata metadata;
    private final MemoryTransactionHandle transactionHandle;

    public TransactionalMemoryMetadata(MemoryMetadata metadata, MemoryTransactionHandle transactionHandle)
    {
        this.metadata = metadata;
        this.transactionHandle = transactionHandle;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metadata.listSchemaNames(session);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        metadata.createSchema(session, schemaName, properties);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        metadata.dropSchema(session, schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return metadata.getTableHandle(session, schemaTableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.getTableMetadata(session, tableHandle);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull)
    {
        return metadata.listTables(session, schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.getColumnHandles(session, tableHandle);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return metadata.listTableColumns(session, prefix);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        metadata.dropTable(session, tableHandle);
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        metadata.renameTable(session, tableHandle, newTableName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        metadata.createTable(session, tableMetadata, ignoreExisting);
    }

    @Override
    public ConnectorTableHandle createTemporaryTable(ConnectorSession session, List<ColumnMetadata> columns, Optional<ConnectorPartitioningMetadata> partitioningMetadata)
    {
        return metadata.createTemporaryTable(transactionHandle, session, columns, partitioningMetadata);
    }

    public ConnectorCommitHandle commit()
    {
        return metadata.commit(transactionHandle);
    }

    @Override
    public MemoryOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return metadata.beginCreateTable(session, tableMetadata, layout);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return metadata.finishCreateTable(session, tableHandle, fragments, computedStatistics);
    }

    @Override
    public MemoryInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return metadata.beginInsert(session, tableHandle);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return metadata.finishInsert(session, insertHandle, fragments, computedStatistics);
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        metadata.createView(session, viewMetadata, viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        metadata.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, String schemaNameOrNull)
    {
        return metadata.listViews(session, schemaNameOrNull);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return metadata.getViews(session, prefix);
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle handle, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
    {
        return metadata.getTableLayouts(session, handle, constraint, desiredColumns);
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return metadata.getTableLayout(session, handle);
    }
}
