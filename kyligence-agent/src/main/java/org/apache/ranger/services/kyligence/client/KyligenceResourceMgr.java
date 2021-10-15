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
import org.apache.ranger.plugin.util.TimedEventUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class KyligenceResourceMgr {

    public static final String PROJECT = "project";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String COLUMN = "column";

    private static final Logger LOG = Logger.getLogger(KyligenceResourceMgr.class);

    public static Map<String, Object> validateConfig(String serviceName, Map<String, String> configs) throws IOException {
        Map<String, Object> ret ;

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
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> resultList = null;
        List<String> projectList = null;
        List<String> databaseList = null;
        List<String> tableList = null;
        List<String> columnList = null;
        String projectName = null;
        String databaseName = null;
        String tableName = null;
        String columnName = null;


        if (LOG.isDebugEnabled()) {
            LOG.debug("<== KyligenceResourceMgr.getKylinResources() UserInput: \"" + userInput + "\" resource : " + resource + " resourceMap: " + resourceMap);
        }

        if (userInput != null && resource != null) {
            if (resourceMap != null && !resourceMap.isEmpty()) {
                projectList = resourceMap.get(PROJECT);
                databaseList = resourceMap.get(DATABASE);
                tableList = resourceMap.get(TABLE);
                columnList = resourceMap.get(COLUMN);
            }
            switch (resource.trim().toLowerCase()) {
                case PROJECT:
                    projectName = userInput;
                    break;
                case DATABASE:
                    databaseName = userInput;
                case TABLE:
                    tableName = userInput;
                    break;
                case COLUMN:
                    columnName = userInput;
                    break;
                default:
                    break;
            }
        }

        if (serviceName != null && userInput != null) {
            try {

                if (LOG.isDebugEnabled()) {
                    LOG.debug("==> KyligenceResourceMgr.getKylinResources() UserInput: \"" + userInput + "\" configs: " + configs + " projectList: " + projectList + " tableList: "
                            + tableList + " columnList: " + columnList);
                }

                final KyligenceClient kyligenceClient = new KyligenceConnectionManager().getKyligenceConnection(serviceName, serviceType, configs);

                Callable<List<String>> callableObj = null;

                final String finalProjectName;
                final String finalDatabaseName;
                final String finalTableName;
                final String finalColumnName;

                final List<String> finalProjectList = projectList;
                final List<String> finalDatabaseList = databaseList;
                final List<String> finalTableList = tableList;
                final List<String> finalColumnList = columnList;

                if (kyligenceClient != null) {
                    if (projectName != null && !projectName.isEmpty()) {
                        finalProjectName = projectName;
                        callableObj = new Callable<List<String>>() {
                            @Override
                            public List<String> call() throws Exception {
                                return kyligenceClient.getProjectList(finalProjectName, finalProjectList);
                            }
                        };
                    } else if (databaseName != null && !databaseName.isEmpty()) {
                        finalDatabaseName = databaseName;
                        callableObj = new Callable<List<String>>() {
                            @Override
                            public List<String> call() throws Exception {
                                return kyligenceClient.getDatabaseList(finalDatabaseName, finalProjectList, finalDatabaseList);
                            }
                        };
                    } else if (tableName != null && !tableName.isEmpty()) {
                        finalTableName = tableName;
                        callableObj = new Callable<List<String>>() {
                            @Override
                            public List<String> call() throws Exception {
                                return kyligenceClient.getTableList(finalTableName, finalProjectList, finalDatabaseList, finalTableList);
                            }
                        };
                    } else if (columnName != null && !columnName.isEmpty()) {
                        // Column names are matched by the wildcardmatcher
                        columnName += "*";
                        finalColumnName = columnName;
                        callableObj = new Callable<List<String>>() {
                            @Override
                            public List<String> call() throws Exception {
                                return kyligenceClient.getColumnList(finalColumnName, finalProjectList, finalDatabaseList, finalTableList, finalColumnList);
                            }
                        };
                    }
                    if (callableObj != null) {
                        synchronized (kyligenceClient) {
                            resultList = TimedEventUtil.timedTask(callableObj, 5, TimeUnit.SECONDS);
                        }
                    } else {
                        LOG.error("Could not initiate a PrestoClient timedTask");
                    }
                }
            } catch (Exception e) {
                LOG.error("Unable to get Kyligence resource", e);
                throw e;
            }
        }
        return resultList;
    }
}
