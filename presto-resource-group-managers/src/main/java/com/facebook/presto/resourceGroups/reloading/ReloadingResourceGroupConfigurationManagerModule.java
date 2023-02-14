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
package com.facebook.presto.resourceGroups.reloading;

import com.facebook.presto.spi.resourceGroups.ResourceGroupConfigurationManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.management.MBeanServer;

import java.lang.management.ManagementFactory;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ReloadingResourceGroupConfigurationManagerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(ReloadingResourceGroupConfig.class);
        binder.bind(ReloadingResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        binder.bind(ResourceGroupConfigurationManager.class).to(ReloadingResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ReloadingResourceGroupConfigurationManager.class).withGeneratedName();
        MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
        binder.bind(MBeanServer.class).toInstance(platformMBeanServer);
    }
}
