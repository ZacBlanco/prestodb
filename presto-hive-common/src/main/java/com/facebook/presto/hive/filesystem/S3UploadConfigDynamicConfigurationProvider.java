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
package com.facebook.presto.hive.filesystem;

import com.facebook.presto.hive.DynamicConfigurationProvider;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HiveCommonSessionProperties;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

public class S3UploadConfigDynamicConfigurationProvider
        implements DynamicConfigurationProvider
{
    @Override
    public void updateConfiguration(Configuration configuration, HdfsContext context, URI uri)
    {
        context.getSession().ifPresent(session -> configuration.set("presto.s3.multipart.min-part-size", Long.toString(HiveCommonSessionProperties.getS3UploadMinPartSize(session))));
    }

    @Override
    public boolean isUriIndependentConfigurationProvider()
    {
        return true;
    }
}