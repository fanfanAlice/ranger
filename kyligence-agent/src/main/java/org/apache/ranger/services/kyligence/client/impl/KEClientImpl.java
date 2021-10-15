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

package org.apache.ranger.services.kyligence.client.impl;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.codec.binary.Base64;
import org.apache.ranger.services.kyligence.client.IClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KEClientImpl implements IClient {

    private final String baseUrl;

    private final String encodeKey;

    public KEClientImpl(String url, String username, String password) {
        this.baseUrl = url + "/kylin/api/";
        this.encodeKey = new String(Base64.encodeBase64((username + ":" + password).getBytes()));
    }

    @Override
    public List<String> getProjectList() throws IOException {
        String url = baseUrl + "projects?pageSize=1000";
        JsonObject data = new JsonParser().parse(sendV4(url)).getAsJsonObject();
        if (null == data)
            throw new RuntimeException("Failed to get Kyligence projects by api: " + url);
        List<String> list = Lists.newArrayList();
        JsonObject object = data.getAsJsonObject("data");
        JsonArray projects = object.getAsJsonArray("projects");
        for (int i = 0; i < projects.size(); i++) {
            JsonObject jsonObject = projects.get(i).getAsJsonObject();
            String name = jsonObject.get("name").getAsString();
            list.add(name);
        }
        return list;
    }

    @Override
    public List<String> getDatabases(String project, String database) throws IOException {
        List<String> list = Lists.newArrayList();
        JsonArray projects = getProjectTableNames(project, database);
        for (int i = 0; i < projects.size(); i++) {
            JsonObject jsonObject = projects.get(i).getAsJsonObject();
            String name = jsonObject.get("dbname").getAsString();
            list.add(name);
        }
        return list;
    }

    @Override
    public List<String> getTables(String project, List<String> database, String table) throws IOException {
        List<String> list = Lists.newArrayList();
        JsonArray projects = getProjectTableNames(project, table);
        for (int i = 0; i < projects.size(); i++) {
            JsonObject jsonObject = projects.get(i).getAsJsonObject();
            String dbname = jsonObject.get("dbname").getAsString();
            if (database.contains(dbname)) {
                JsonArray tables = jsonObject.getAsJsonArray("tables");
                for (int j = 0; j < tables.size(); j++) {
                    String table_name = tables.get(j).getAsJsonObject().get("table_name").getAsString();
                    list.add(table_name);
                }
            }
        }
        return list;
    }

    private JsonArray getProjectTableNames(String project, String table) throws IOException {
        String url = String.format(baseUrl + "tables/project_table_names?project=%s&data_source_type=9&table=%s&page_offset=0&page_size=1000", project, table);
        JsonObject data = new JsonParser().parse(sendV4(url)).getAsJsonObject();
        if (null == data)
            throw new RuntimeException("Failed to get Kyligence projects by api: " + url);
        JsonObject object = data.getAsJsonObject("data");
        return object.getAsJsonArray("databases");
    }

    @Override
    public List<String> getColumns(String project, String database, String table, String column) throws IOException {
        String url = String.format(baseUrl + "tables?project=%s&database=%s&table=%s&is_fuzzy=false&source_type=9&ext=true", project, database, table);
        JsonObject data = new JsonParser().parse(sendV4(url)).getAsJsonObject();
        if (null == data)
            throw new RuntimeException("Failed to get Kyligence projects by api: " + url);
        List<String> list = Lists.newArrayList();
        JsonObject object = data.getAsJsonObject("data");
        JsonArray columns = object.getAsJsonArray("tables").get(0).getAsJsonObject().getAsJsonArray("columns");
        for (int i = 0; i < columns.size(); i++) {
            JsonObject jsonObject = columns.get(i).getAsJsonObject();
            String name = jsonObject.get("name").getAsString();
            if (column == null || name.contains(column.toUpperCase())) {
                list.add(name);
            }
        }
        return list;
    }

    private String sendV4(String urlStr) throws IOException {
        return send(urlStr, "GET", null, "application/vnd.apache.kylin-v4+json");
    }

    private String send(String urlStr, String method, String data, String header) throws IOException {
        HttpURLConnection connect = null;
        InputStream is = null;
        OutputStream os = null;
        BufferedReader br = null;
        try {
            URL url = new URL(urlStr);
            connect = (HttpURLConnection) url.openConnection();
            connect.setRequestMethod(method);
            connect.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            connect.setRequestProperty("Accept", header);
            connect.setRequestProperty("Accept-Language", "en");
            connect.setRequestProperty("Authorization", "Basic " + this.encodeKey);
            connect.setConnectTimeout(500000);
            connect.setReadTimeout(500000);
            connect.setUseCaches(false);

            if (null != data) {
                connect.setDoInput(true);
                connect.setDoOutput(true);
                os = connect.getOutputStream();
                os.write(data.getBytes());
            }

            if (connect.getResponseCode() != 200) {
                throw new IllegalStateException("Response code is not 200, Response code is:" + connect.getResponseCode());
            }
            StringBuilder sb = new StringBuilder();

            is = connect.getInputStream();
            br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line;

            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (ProtocolException e) {
            throw new ProtocolException("ProtocolException, please check kyligence config!");
        } catch (MalformedURLException e) {
            throw new MalformedURLException("MalformedURLException, please check kyligence config!");
        } catch (IOException e) {
            throw new IOException("IOException, please check kyligence config!");
        } catch (RuntimeException e) {
            throw new RuntimeException("Server unavailable: " + e.getMessage());
        } finally {
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != os) {
                try {
                    os.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != connect) {
                connect.disconnect();
            }
        }
    }
}
