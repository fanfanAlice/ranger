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
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.client.HadoopException;
import org.apache.ranger.services.kyligence.client.impl.KEClientImpl;

import javax.security.auth.Subject;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.*;

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
        this.password = configs.get("password");

        if (StringUtils.isEmpty(this.kylinUrl)) {
            LOG.info("No value found for configuration 'kyligence.url'. kyligence resource lookup will fail.");
        }
        if (StringUtils.isEmpty(this.userName)) {
            LOG.info("No value found for configuration 'username'. kyligence resource lookup will fail.");
        }
        if (StringUtils.isEmpty(this.password)) {
            LOG.info("No value found for configuration 'password'. kyligence resource lookup will fail.");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("kyligence client is build with url [" + this.kylinUrl + "], user: [" + this.userName
                    + "], password: [" + "*********" + "].");
        }
    }

    public List<String> getProjectList(final String projectMatching, final List<String> existingProjects) throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kyligence project list for projectMatching: " + projectMatching + ", existingProjects: "
                    + existingProjects);
        }
        Subject subj = getLoginSubject();
        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                IClient client = new KEClientImpl(kylinUrl, userName, password);
                List<String> projectList = null;
                try {
                    projectList = client.getProjectList();
                } catch (IOException ioException) {
                    LOG.error("<== KyligenceClient.getProjectList() :Unable to get the Project List", ioException);
                }
                if (CollectionUtils.isEmpty(projectList)) {
                    return Collections.emptyList();
                }
                return getProjectFromResponse(projectMatching, existingProjects, projectList);
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kylin project list result: " + ret);
        }
        return ret;
    }

    private static List<String> getProjectFromResponse(String projectMatching, List<String> existingProjects,
                                                       List<String> projectList) {
        List<String> projcetNames = new ArrayList<String>();
        for (String projectName : projectList) {
            if (CollectionUtils.isNotEmpty(existingProjects) && existingProjects.contains(projectName)) {
                continue;
            }
            if (StringUtils.isEmpty(projectMatching) || projectMatching.startsWith("*")
                    || projectName.toLowerCase().startsWith(projectMatching.toLowerCase())) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("getProjectFromResponse(): Adding kylin project " + projectName);
                }
                projcetNames.add(projectName);
            }
        }
        return projcetNames;
    }

    public static Map<String, Object> connectionTest(String serviceName, Map<String, String> configs) throws IOException {
        KyligenceClient kylinClient = getKylinClient(serviceName, configs);
        List<String> strList = kylinClient.getProjectList(null, null);

        boolean connectivityStatus = false;
        if (CollectionUtils.isNotEmpty(strList)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("ConnectionTest list size" + strList.size() + " kylin projects");
            }
            connectivityStatus = true;
        }

        Map<String, Object> responseData = new HashMap<String, Object>();
        if (connectivityStatus) {
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(connectivityStatus, successMsg, successMsg, null, null, responseData);
        } else {
            String failureMsg = "Unable to retrieve any kylin projects using given parameters.";
            BaseClient.generateResponseDataMap(connectivityStatus, failureMsg, failureMsg + ERROR_MESSAGE, null, null,
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

    public List<String> getDatabaseList(String finalSchemaName, List<String> projectList, List<String> databases) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kyligence database list for databaseMatching: " + finalSchemaName + ", existingDatabases: "
                    + databases);
        }
        Subject subj = getLoginSubject();
        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                IClient client = new KEClientImpl(kylinUrl, userName, password);
                List<String> databaseList = Lists.newLinkedList();
                projectList.forEach(x -> {
                    try {
                        databaseList.addAll(client.getDatabases(x, finalSchemaName));
                    } catch (IOException ioException) {
                        LOG.error("<== KyligenceClient.getDatabaseList() :Unable to get the Database List", ioException);
                    }
                });
                if (CollectionUtils.isEmpty(databaseList)) {
                    return Collections.emptyList();
                }
                databases.forEach(x -> {
                    if (!databaseList.contains(x)) {
                        databaseList.add(x);
                    }
                });
                return databaseList;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kylin database list result: " + ret);
        }
        return ret;
    }

    public List<String> getTableList(String finalTableName, List<String> finalProjectList, List<String> finalDatabaseList, List<String> finalTableList) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kyligence table list for databaseMatching: " + finalTableName + ", existingTables: "
                    + finalTableList);
        }
        Subject subj = getLoginSubject();
        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                IClient client = new KEClientImpl(kylinUrl, userName, password);
                List<String> tableLists = Lists.newLinkedList();
                finalProjectList.forEach(x -> {
                    try {
                        tableLists.addAll(client.getTables(x, finalDatabaseList, finalTableName));
                    } catch (IOException ioException) {
                        LOG.error("<== KyligenceClient.getTableList() :Unable to get the Table List", ioException);
                    }
                });
                if (CollectionUtils.isEmpty(tableLists)) {
                    return Collections.emptyList();
                }
                finalTableList.forEach(x -> {
                    if (!tableLists.contains(x)) {
                        tableLists.add(x);
                    }
                });
                return tableLists;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kylin table list result: " + ret);
        }
        return ret;
    }

    public List<String> getColumnList(String finalColumnName, List<String> finalProjectList, List<String> finalDatabaseList, List<String> finalTableList, List<String> finalColumnList) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kyligence table list for columnMatching: " + finalColumnName + ", existingTables: "
                    + finalColumnList);
        }
        Subject subj = getLoginSubject();
        if (subj == null) {
            return Collections.emptyList();
        }

        List<String> ret = Subject.doAs(subj, new PrivilegedAction<List<String>>() {
            @Override
            public List<String> run() {
                IClient client = new KEClientImpl(kylinUrl, userName, password);
                List<String> columnLists = Lists.newLinkedList();
                finalProjectList.forEach(x -> {
                    try {
                        columnLists.addAll(client.getColumns(x, finalDatabaseList.get(0), finalTableList.get(0), finalColumnName));
                    } catch (IOException ioException) {
                        LOG.error("<== KyligenceClient.getColumnList() :Unable to get the column List", ioException);
                    }
                });
                if (CollectionUtils.isEmpty(columnLists)) {
                    return Collections.emptyList();
                }
                finalColumnList.forEach(x -> {
                    if (!columnLists.contains(x)) {
                        columnLists.add(x);
                    }
                });
                return columnLists;
            }
        });

        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting kylin column list result: " + ret);
        }
        return ret;
    }
}
