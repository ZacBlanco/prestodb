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
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.block.MethodHandleUtil.methodHandle;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;

public class CreateTableSampleProcedure
        implements Provider<Procedure>
{
    private static final Logger LOG = Logger.get(CreateTableSampleProcedure.class);

    private static final MethodHandle CREATE_TABLE_SAMPLE = methodHandle(
            CreateTableSampleProcedure.class,
            "createTableSample",
            ConnectorSession.class,
            String.class,
            String.class);

    @Inject
    public CreateTableSampleProcedure(
            IcebergConfig config,
            IcebergMetadataFactory metadataFactory,
            HdfsEnvironment hdfsEnvironment,
            IcebergResourceFactory resourceFactory)
    {
//        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
//        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
//        this.resourceFactory = requireNonNull(resourceFactory, "resourceFactory is null");
//        requireNonNull(config, "config is null");
//        this.catalogType = config.getCatalogType();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "create_table_sample",
                ImmutableList.of(
                        new Procedure.Argument("schema", VARCHAR),
                        new Procedure.Argument("table", VARCHAR)),
                CREATE_TABLE_SAMPLE.bindTo(this));
    }

    public void createTableSample(ConnectorSession clientSession, String schema, String table)
    {
        // TODO(zac) implement sample procedure
        throw new RuntimeException("Procedure not yet implemented");
    }
}
