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
package com.facebook.presto.prestox;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.connector.jmx.JmxPlugin;
import com.facebook.presto.iceberg.IcebergFileFormat;
import com.facebook.presto.iceberg.IcebergPlugin;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class PrestoXQueryRunner
{
    private static final Logger log = Logger.get(PrestoXQueryRunner.class);

    private PrestoXQueryRunner()
    {
    }

    public static DistributedQueryRunner createQueryRunner(OptionalInt nodeCount, OptionalInt httpPort)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("org.apache.iceberg", WARN);
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session);
        queryRunnerBuilder.setExtraProperties(ImmutableMap.<String, String>of("http-server.http.port", Integer.toString(httpPort.orElse(8080))));
        queryRunnerBuilder.setNodeCount(nodeCount.orElse(1));
        queryRunnerBuilder.setExternalWorkerLauncher(Optional.of((idx, discoveryUri) -> {
            Path tempDirectoryPath = null;
            try {
                tempDirectoryPath = Files.createTempDirectory(PrestoXQueryRunner.class.getSimpleName());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            log.info("Temp directory for Worker #%d: %s", idx, tempDirectoryPath.toString());
            int port = 1234 + idx;
            String binaryPath = Paths.get(System.getProperty("user.dir")).resolve("presto-oxidized/prestox/target/debug/server").toFile().toString();
            ProcessBuilder builder = new ProcessBuilder(binaryPath);
            builder.inheritIO();
            Map<String, String> env = builder.environment();
            env.put("PRESTOX_DISCOVERY_URI", discoveryUri.toString());
            env.put("PRESTOX_HTTP_SERVER_HTTP_PORT", Integer.toString(port));
            env.put("RUST_BACKTRACE", Integer.toString(1));
            try {
                return builder.start();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));

        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();

        queryRunner.installPlugin(new IcebergPlugin());

        Map<String, String> extraConnectorProperties = ImmutableMap.of();
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", HADOOP.name())
                .put("iceberg.file-format", IcebergFileFormat.PARQUET.name())
                .put("iceberg.catalog.warehouse", dataDirectory.getParent().toFile().toURI().toString())
                .putAll(extraConnectorProperties)
                .build();

        queryRunner.createCatalog("iceberg", "iceberg", icebergProperties);
        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");
        return queryRunner;
    }

    public static void main(String[] args)
    {
        Logging.initialize();
        Object wait = new Object();
        try (DistributedQueryRunner queryRunner = createQueryRunner(OptionalInt.of(0), OptionalInt.of(8080))) {
            log.info("======== SERVER STARTED ========");
            log.info("====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
            synchronized (wait) {
                wait.wait();
            }
        }
        catch (Exception e) {
            log.error("Failed to start query runner: %s", e);
            System.exit(1);
        }
        finally {
            System.exit(0);
        }
    }
}
