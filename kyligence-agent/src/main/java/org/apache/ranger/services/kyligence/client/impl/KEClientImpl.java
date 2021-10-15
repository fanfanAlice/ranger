package org.apache.ranger.services.kyligence.client.impl;

import com.google.common.collect.Lists;
import com.google.gson.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.ranger.services.kyligence.client.IClient;
import org.apache.ranger.services.kyligence.client.json.model.AclTCRRequest;
import org.apache.ranger.services.kyligence.client.json.model.AclTCRResponse;
import org.apache.ranger.services.kyligence.client.json.model.ProjectPermission;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class KEClientImpl implements IClient {

    private static final Logger LOG = Logger.getLogger(KEClientImpl.class);

    private final String baseUrl;

    private final String encodeKey;

    public KEClientImpl(String url, String username, String password) {
        this.baseUrl = url + "/kylin/api/";
        this.encodeKey = new String(Base64.encodeBase64((username + ":" + password).getBytes()));
    }

    @Override
    public List<String> getProjectList() throws IOException {
        String url = baseUrl + "projects?pageSize=1000";
        JsonObject data = new JsonParser().parse(sendV2(url)).getAsJsonObject();
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

    @Override
    public List<ProjectPermission> getProjectPermission(String project, String userOrGroupName) throws Exception {
        String url = String.format(this.baseUrl + "access/project?project=%s&name=%s&page_size=1000", project, userOrGroupName);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "GET", null)).getAsJsonObject();
        if (null == data)
            throw new RuntimeException("Failed to get Kyligence getProjectPermission by api: " + url);
        List<ProjectPermission> projectPermissions = Lists.newLinkedList();
        JsonObject object = data.getAsJsonObject("data");
        JsonArray value = object.getAsJsonArray("value");
        for (int i = 0; i < value.size(); i++) {
            ProjectPermission projectPermission = new Gson().fromJson(value.get(i), ProjectPermission.class);
            projectPermissions.add(projectPermission);
        }
        return projectPermissions;
    }

    @Override
    public void grantProjectPermission(String project, String type, String permission, String[] names) throws Exception {
        String url = this.baseUrl + "access/project";
        JsonObject object = new JsonObject();
        object.addProperty("project", project);
        object.addProperty("type", type);
        object.addProperty("permission", permission);
        JsonArray jArray = new JsonArray();
        for (String x : names) {
            jArray.add(new JsonPrimitive(x));
        }
        object.add("names", jArray);
        String s = new Gson().toJson(object);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "POST", s)).getAsJsonObject();
        if ("000".equals(data.get("code").getAsString())) {
            LOG.info("grantProjectPermission success !");
        } else {
            LOG.info("grantProjectPermission failed ! project = " + project + ", type = " + type + ", permission = " + permission + ", names = " + Arrays.toString(names));
        }
    }

    @Override
    public void updateProjectPermission(String project, String type, String permission, String name) throws Exception {
        String url = this.baseUrl + "access/project";
        JsonObject object = new JsonObject();
        object.addProperty("project", project);
        object.addProperty("type", type);
        object.addProperty("permission", permission);
        object.addProperty("name", name);
        String s = new Gson().toJson(object);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "PUT", s)).getAsJsonObject();
        if ("000".equals(data.get("code").getAsString())) {
            LOG.info("updateProjectPermission success !");
        } else {
            LOG.info("updateProjectPermission failed ! project = " + project + ", type = " + type + ", permission = " + permission + ", name = " + name);
        }
    }

    @Override
    public void deleteProjectPermission(String project, String type, String name) throws Exception {
        String url = String.format(this.baseUrl + "access/project?project=%s&type=%s&name=%s", project, type, name);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "DELETE", null)).getAsJsonObject();
        if ("000".equals(data.get("code").getAsString())) {
            LOG.info("deleteProjectPermission success !");
        } else {
            LOG.info("deleteProjectPermission failed ! project = " + project + ", type = " + type + ", name = " + name);
        }
    }

    @Override
    public List<AclTCRResponse> getAclList(String type, String name, String project, boolean authorized_only) throws Exception {
        String url = String.format(this.baseUrl + "acl/%s/%s?authorized_only=%s&project=%s", type, name, authorized_only, project);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "GET", null)).getAsJsonObject();
        if (null == data)
            throw new RuntimeException("Failed to get Kyligence getAclList by api: " + url);
        List<AclTCRResponse> aclTCRResponseList = Lists.newLinkedList();
        JsonArray array = data.getAsJsonArray("data");
        for (int i=0; i<array.size(); i++) {
            AclTCRResponse aclTCRResponse = new Gson().fromJson(array.get(i).getAsJsonObject(), AclTCRResponse.class);
            aclTCRResponseList.add(aclTCRResponse);
        }
        return aclTCRResponseList;
    }

    @Override
    public void updateAcl(String type, String name, String project, AclTCRRequest request) throws Exception {
        String url = String.format(this.baseUrl + "acl/%s/%s?project=%s", type, name, project);
        String s = new Gson().toJson(request);
        JsonObject data = new JsonParser().parse(sendV4Public(url, "PUT", s)).getAsJsonObject();
        if ("000".equals(data.get("code").getAsString())) {
            LOG.info("updateAcl success !");
        } else {
            LOG.info("updateAcl failed ! project = " + project + ", type = " + type + ", name = " + name +", request { " + request.toString() + " }");
        }


    }

    private String sendV2(String urlStr) throws IOException {
        return send(urlStr, "GET", null, "application/vnd.apache.kylin-v2+json");
    }

    private String sendV4Public(String urlStr, String method, String data) throws IOException {
        return send(urlStr, method, data, "application/vnd.apache.kylin-v4-public+json");
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
