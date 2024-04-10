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
package com.facebook.presto.hive;

import com.facebook.presto.hive.filesystem.S3UploadConfigDynamicConfigurationProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.google.inject.multibindings.Multibinder.newSetBinder;

public class HiveCommonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(HiveCommonClientConfig.class);
        binder.bind(HiveCommonSessionProperties.class).in(Scopes.SINGLETON);
        Multibinder<DynamicConfigurationProvider> setBinder = newSetBinder(binder, DynamicConfigurationProvider.class);
        setBinder.addBinding().to(S3UploadConfigDynamicConfigurationProvider.class).in(Scopes.SINGLETON);
    }
}
