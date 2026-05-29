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
package com.facebook.presto.server.protocol;

import com.facebook.presto.spi.QueryId;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import org.testng.annotations.Test;

import java.lang.reflect.Proxy;
import java.net.URI;

import static org.testng.Assert.assertEquals;

public class TestStatementResourceV2
{
    @Test
    public void testQueuedUriCanBeGeneratedForV2StatementPath()
    {
        URI uri = QueryResourceUtil.getQueuedUri(
                new QueryId("query_1"),
                "slug",
                1,
                uriInfo(),
                "https",
                null,
                true,
                "/v2/statement");

        assertEquals(uri.toString(), "https://coordinator.example.com:8080/v2/statement/queued/query_1/1?slug=slug&binaryResults=true");
    }

    @Test
    public void testQueuedUriDefaultPathRemainsV1StatementPath()
    {
        URI uri = QueryResourceUtil.getQueuedUri(
                new QueryId("query_1"),
                "slug",
                1,
                uriInfo(),
                "https",
                null,
                false);

        assertEquals(uri.toString(), "https://coordinator.example.com:8080/v1/statement/queued/query_1/1?slug=slug");
    }

    private static UriInfo uriInfo()
    {
        return (UriInfo) Proxy.newProxyInstance(
                TestStatementResourceV2.class.getClassLoader(),
                new Class<?>[] {UriInfo.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getBaseUriBuilder":
                            return UriBuilder.fromUri("http://coordinator.example.com:8080/");
                        case "getRequestUri":
                            return URI.create("http://coordinator.example.com:8080/v2/statement");
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }
}
