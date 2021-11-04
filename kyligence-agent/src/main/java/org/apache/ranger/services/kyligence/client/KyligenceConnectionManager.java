/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.services.kyligence.client;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.util.TimedEventUtil;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class KyligenceConnectionManager {

    private static final Logger LOG = Logger.getLogger(KyligenceConnectionManager.class);

    protected ConcurrentMap<String, KyligenceClient> kyligenceConnectionCache;
    protected ConcurrentMap<String, Boolean> repoConnectStatusMap;

    public KyligenceConnectionManager() {
        kyligenceConnectionCache = new ConcurrentHashMap<>();
        repoConnectStatusMap = new ConcurrentHashMap<>();
    }

    public KyligenceClient getKyligenceConnection(final String serviceName, final String serviceType, final Map<String, String> configs) {
        KyligenceClient kyligenceClient = null;

        if (serviceType != null) {
            kyligenceClient = kyligenceConnectionCache.get(serviceName);
            if (kyligenceClient == null) {
                if (configs != null) {
                    final Callable<KyligenceClient> connectKyligence = new Callable<KyligenceClient>() {
                        @Override
                        public KyligenceClient call() throws Exception {
                            return new KyligenceClient(serviceName, configs);
                        }
                    };
                    try {
                        kyligenceClient = TimedEventUtil.timedTask(connectKyligence, 5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        LOG.error("Error connecting to Presto repository: " +
                                serviceName + " using config: " + configs, e);
                    }

                    KyligenceClient oldClient = null;
                    if (kyligenceClient != null) {
                        oldClient = kyligenceConnectionCache.putIfAbsent(serviceName, kyligenceClient);
                    } else {
                        oldClient = kyligenceConnectionCache.get(serviceName);
                    }

                    if (oldClient != null) {
                        kyligenceClient = oldClient;
                    }
                    repoConnectStatusMap.put(serviceName, true);
                } else {
                    LOG.error("Connection Config not defined for asset :"
                            + serviceName, new Throwable());
                }
            } else {
                try {
                    kyligenceClient.getProjectList("");
                } catch (Exception e) {
                    kyligenceConnectionCache.remove(serviceName);
                    kyligenceClient = getKyligenceConnection(serviceName, serviceType, configs);
                }
            }
        } else {
            LOG.error("Asset not found with name " + serviceName, new Throwable());
        }
        return kyligenceClient;
    }
}
