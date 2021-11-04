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
import org.apache.ranger.plugin.service.ResourceLookupContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class KyligenceResourceMgr {

    public static final String PROJECT = "project";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String COLUMN = "column";

    private static final Logger LOG = Logger.getLogger(KyligenceResourceMgr.class);

    public static Map<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws IOException {
        Map<String, Object> ret;

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> KyligenceResourceMgr.validateConfig ServiceName: " + serviceName + "Configs" + configs);
        }

        try {
            ret = KyligenceClient.connectionTest(serviceName, configs);
        } catch (Exception e) {
            LOG.info("<== KyligenceResourceMgr.validateConfig Error: " + e);
            throw e;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== KyligenceResourceMgr.validateConfig Result: " + ret);
        }
        return ret;
    }

    public static List<String> getKylinResources(String serviceName, String serviceType, Map<String, String> configs,
                                                 ResourceLookupContext context) throws Exception {
        String userInput = context.getUserInput();
        String resourceName = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> resultList = new LinkedList<>();
        if (userInput != null && resourceName != null) {
            final KyligenceClient kyligenceClient = new KyligenceConnectionManager().getKyligenceConnection(serviceName, serviceType, configs);
            switch (resourceName) {
                case PROJECT:
                    resultList = kyligenceClient.getProjectList(userInput);
                    break;
                case DATABASE: {
                    String project = resourceMap.get(PROJECT).get(0);
                    resultList = kyligenceClient.getDatabaseList(userInput, project);
                    break;
                }
                case TABLE: {
                    String project = resourceMap.get(PROJECT).get(0);
                    String database = resourceMap.get(DATABASE).get(0);
                    resultList = kyligenceClient.getTableList(userInput, project, database);
                    break;
                }
                case COLUMN: {
                    String project = resourceMap.get(PROJECT).get(0);
                    String database = resourceMap.get(DATABASE).get(0);
                    String table = resourceMap.get(TABLE).get(0);
                    resultList = kyligenceClient.getColumnList(userInput, project, database, table);
                    break;
                }
            }
        }

        return resultList;
    }
}
