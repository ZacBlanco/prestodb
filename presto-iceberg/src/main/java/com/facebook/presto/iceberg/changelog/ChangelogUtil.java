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
package com.facebook.presto.iceberg.changelog;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.List;

import static com.facebook.presto.iceberg.TypeConverter.toIcebergType;
import static com.facebook.presto.iceberg.TypeConverter.toPrestoType;

public class ChangelogUtil
{
    private ChangelogUtil() {}

    public static Schema changelogTableSchema(Type primaryKeyType)
    {
        return new Schema(
                Types.NestedField.required(0, "operation", Types.StringType.get()),
                Types.NestedField.required(1, "ordinal", Types.LongType.get()),
                Types.NestedField.required(2, "snapshotId", Types.LongType.get()),
                Types.NestedField.required(3, "primary_key", primaryKeyType));
    }

    public static ConnectorTableMetadata getChangelogTableMeta(SchemaTableName tableName, Type primaryKeyType, TypeManager typeManager)
    {
        return new ConnectorTableMetadata(tableName, getColumnMetadata(typeManager, primaryKeyType));
    }

    public static Type getPrimaryKeyType(Table table, String columnName)
    {
        return table.schema().findType(columnName);
    }

    public static Type getPrimaryKeyType(ConnectorTableMetadata table, String columnName)
    {
        return toIcebergType(table.getColumns().stream().filter(x -> x.getName().equals(columnName)).findFirst().get().getType());
    }

    public static List<ColumnMetadata> getColumnMetadata(TypeManager typeManager, Type primaryKeyType)
    {
        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
        changelogTableSchema(primaryKeyType).columns().stream()
                .map(x -> new ColumnMetadata(x.name(), toPrestoType(x.type(), typeManager)))
                .forEach(builder::add);
        return builder.build();
    }
}
