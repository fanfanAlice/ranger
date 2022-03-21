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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.kyligence.client.impl.KEClientImpl;

import javax.security.auth.Subject;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KyligenceClient extends BaseClient {

    private static final Logger LOG = Logger.getLogger(KyligenceClient.class);

    private static final String ERROR_MESSAGE = " You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check ranger_admin.log for more info.";

    private String kylinUrl;
    private String userName;
    private String password;

    public KyligenceClient(String serviceName, Map<String, String> configs) {

        super(serviceName, configs, "kyligence-client");

        this.kylinUrl = configs.get("kyligence.url");
        this.userName = configs.get("username");
        this.password = configs.get("kyligence.password");
    }

    public List<String> getProjectList(final String projectMatching) {
        Subject subj = getLoginSubject();
        if (subj == null) {
            return Collections.emptyList();
        }

        IClient client = new KEClientImpl(kylinUrl, userName, password);
        List<String> projectList = client.getProjectList(projectMatching);
        if (CollectionUtils.isEmpty(projectList)) {
            return Collections.emptyList();
        }
        return projectList;
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws IOException {
        KyligenceClient kylinClient = getKylinClient(serviceName, configs);
        List<String> strList = kylinClient.getProjectList("");

        boolean connectivityStatus = false;
        if (CollectionUtils.isNotEmpty(strList)) {
            LOG.info("ConnectionTest list size" + strList.size() + " kylin projects");
            connectivityStatus = true;
        }

        Map<String, Object> responseData = new HashMap<String, Object>();
        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any kylin projects using given parameters.";
            BaseClient.generateResponseDataMap(false, failureMsg, failureMsg + ERROR_MESSAGE, null, null,
                    responseData);
        }

        return responseData;
    }

    public static KyligenceClient getKylinClient(String serviceName, Map<String, String> configs) {
        KyligenceClient kylinClient;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting KylinClient for datasource: " + serviceName);
        }
        if (MapUtils.isEmpty(configs)) {
            String msgDesc = "Could not connect kylin as connection configMap is empty.";
            LOG.info(msgDesc);
            HadoopException hdpException = new HadoopException(msgDesc);
            hdpException.generateResponseDataMap(false, msgDesc, msgDesc + ERROR_MESSAGE, null, null);
            throw hdpException;
        } else {
            kylinClient = new KyligenceClient(serviceName, configs);
        }
        return kylinClient;
    }

    public List<String> getDatabaseList(String finalSchemaName, String project) {
        List<String> databaseList = Lists.newLinkedList();
        LOG.info("KyligenceClient getDatabaseList finalSchemaName: " + finalSchemaName + ", project: " + project);
        IClient client = new KEClientImpl(kylinUrl, userName, password);
        databaseList.addAll(client.getDatabases(project, finalSchemaName));
        LOG.info("Kyligence database size : " + databaseList.size());
        if (CollectionUtils.isEmpty(databaseList)) {
            return Collections.emptyList();
        }
        return databaseList;
    }

    public List<String> getTableList(String finalTableName, String project, List<String> databases) {
        LOG.info("KyligenceClient getTableList finalTableName: " + finalTableName + ", project: " + project + ", database: " + databases);
        IClient client = new KEClientImpl(kylinUrl, userName, password);
        List<String> tableLists = Lists.newLinkedList();
        List<String> tables = client.getTables(project, databases, finalTableName);
        tableLists.addAll(tables);
        LOG.info("Kyligence table size is: " + tableLists.size());
        if (CollectionUtils.isEmpty(tableLists)) {
            return Collections.emptyList();
        }
        return tableLists;
    }

    public List<String> getColumnList(String finalColumnName, String project, List<String> databases, List<String> tables) {
        LOG.info("KyligenceClient getColumnList finalColumnName: " + finalColumnName + ", project: " + project + ", database: " + databases + ", table: " + tables);
        IClient client = new KEClientImpl(kylinUrl, userName, password);
        List<String> columnLists = Lists.newLinkedList();
        List<String> columns = client.getColumns(project, databases, tables, finalColumnName);
        columnLists.addAll(columns);
        LOG.info("Kyligence column size is: " + columnLists.size());
        if (CollectionUtils.isEmpty(columnLists)) {
            return Collections.emptyList();
        }
        return columnLists;
    }
}
