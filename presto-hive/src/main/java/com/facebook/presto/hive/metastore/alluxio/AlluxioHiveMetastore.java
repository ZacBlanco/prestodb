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

import alluxio.client.table.TableMasterClient;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.table.Constraint;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.PartitionStatistics;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil;
import com.facebook.presto.spi.NotFoundException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.Type;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static java.util.function.Function.identity;

/**
 * Implementation of the {@link HiveMetastore} interface through Alluxio.
 */
public class AlluxioHiveMetastore
        implements ExtendedHiveMetastore
{
    private TableMasterClient client;

    @Inject
    public AlluxioHiveMetastore(TableMasterClient client)
    {
        this.client = client;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getDatabase(databaseName)));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return client.getAllDatabases();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return Optional.of(ProtoUtils.fromProto(client.getTable(databaseName, tableName)));
        }
        catch (NotFoundException e) {
            return Optional.empty();
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        throw new UnsupportedOperationException("getSupportedColumnStatistics");
    }

    @Override
    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        try {
            Table table = getTable(databaseName, tableName)
                    .orElseThrow(() -> new PrestoException(HIVE_METASTORE_ERROR,
                            String.format("Could not retrieve table %s.%s", databaseName, tableName)));
            HiveBasicStatistics basicStats =
                    ThriftMetastoreUtil.getHiveBasicStatistics(table.getParameters());
            // TODO implement logic to populate Map<string, HiveColumnStatistics>
            return new PartitionStatistics(basicStats, Collections.emptyMap());
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName,
            String tableName, Set<String> partitionNames)
    {
        // TODO implement partition statistics
        // currently returns a map of partitionName to empty statistics to satisfy presto requirements
        return Collections.unmodifiableMap(
                partitionNames.stream().collect(Collectors.toMap(identity(), (p) -> PartitionStatistics.empty())));
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName,
            Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updateTableStatistics");
    }

    @Override
    public void updatePartitionStatistics(String databaseName, String tableName,
            String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        throw new UnsupportedOperationException("updatePartitionStatistics");
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        try {
            return Optional.of(client.getAllTables(databaseName));
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        // TODO: Add views on the server side
        return Optional.of(Collections.emptyList());
    }

    @Override
    public void createDatabase(Database database)
    {
        throw new UnsupportedOperationException("createDatabase");
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        throw new UnsupportedOperationException("dropDatabase");
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new UnsupportedOperationException("renameDatabase");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("createTable");
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        throw new UnsupportedOperationException("dropTable");
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException("replaceTable");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new UnsupportedOperationException("renameTable");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new UnsupportedOperationException("addColumn");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new UnsupportedOperationException("renameColumn");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new UnsupportedOperationException("dropColumn");
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName,
            List<String> partitionValues)
    {
        throw new UnsupportedOperationException("getPartition");
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        throw new UnsupportedOperationException("getPartitionNames");
    }

    /**
     * return a list of partition names by which the values of each partition is at least
     * contained which the {@code parts} argument
     *
     * @param databaseName
     * @param tableName
     * @param parts        list of values which returned partitions should contain
     * @return optionally, a list of strings where each entry is in the form of {key}={value}
     */
    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName,
            List<String> parts)
    {
        try {
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName))
                    // Filter out any partitions which have values that don't match
                    .filter(partition -> {
                        List<String> values = partition.getValuesList();
                        if (values.size() != parts.size()) {
                            return false;
                        }
                        for (int i = 0; i < values.size(); i++) {
                            String constraintPart = parts.get(i);
                            if (!constraintPart.isEmpty() && !values.get(i).equals(constraintPart)) {
                                return false;
                            }
                        }
                        return true;
                    })
                    .collect(Collectors.toList());
            List<String> partitionNames = partitionInfos.stream().map(PartitionInfo::getPartitionName).collect(Collectors.toList());
            return Optional.of(partitionNames);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(String databaseName,
            String tableName, List<String> partitionNames)
    {
        if (partitionNames.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            // Get all partitions
            List<PartitionInfo> partitionInfos = ProtoUtils.toPartitionInfoList(
                    client.readTable(databaseName, tableName, Constraint.getDefaultInstance()));
            // Check that table name is correct
            // TODO also check for database name equality
            partitionInfos = partitionInfos.stream().filter(p -> p.getTableName().equals(tableName))
                    .collect(Collectors.toList());
            Map<String, Optional<Partition>> result = partitionInfos.stream()
                    .filter(p -> partitionNames.stream().anyMatch(p.getPartitionName()::equals))
                    .collect(Collectors.toMap(
                            PartitionInfo::getPartitionName,
                            pi -> Optional.of(ProtoUtils.fromProto(pi))));
            return Collections.unmodifiableMap(result);
        }
        catch (AlluxioStatusException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName,
            List<PartitionWithStatistics> partitions)
    {
        throw new UnsupportedOperationException("addPartitions");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts,
            boolean deleteData)
    {
        throw new UnsupportedOperationException("dropPartition");
    }

    @Override
    public void alterPartition(String databaseName, String tableName,
            PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("alterPartition");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException("createRole");
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException("dropRole");
    }

    @Override
    public Set<String> listRoles()
    {
        throw new UnsupportedOperationException("listRoles");
    }

    @Override
    public void grantRoles(Set<String> roles, Set<PrestoPrincipal> grantees,
            boolean withAdminOption, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("grantRoles");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<PrestoPrincipal> grantees,
            boolean adminOptionFor, PrestoPrincipal grantor)
    {
        throw new UnsupportedOperationException("revokeRoles");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("listRoleGrants");
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, PrestoPrincipal grantee,
            Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("grantTablePrivileges");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName,
            PrestoPrincipal grantee, Set<HivePrivilegeInfo> privileges)
    {
        throw new UnsupportedOperationException("revokeTablePrivileges");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName,
            PrestoPrincipal principal)
    {
        throw new UnsupportedOperationException("listTablePrivileges");
    }
}
