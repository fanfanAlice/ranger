package org.apache.ranger.services.kyligence.client;

import org.apache.ranger.services.kyligence.client.json.model.AclTCRRequest;
import org.apache.ranger.services.kyligence.client.json.model.AclTCRResponse;
import org.apache.ranger.services.kyligence.client.json.model.ProjectPermission;

import java.io.IOException;
import java.util.List;

public interface IClient {

    List<String> getProjectList() throws IOException;

    List<String> getDatabases(String project, String database) throws IOException;

    List<String> getTables(String project, List<String> database, String table) throws IOException;

    List<String> getColumns(String project, String database, String table, String column) throws IOException;

    //获取项目级访问权限
    List<ProjectPermission> getProjectPermission(String project, String userOrGroupName) throws Exception;

    //授予项目级访问权限
    void grantProjectPermission(String project, String type, String permission, String[] names) throws Exception;

    //更新项目级访问权限
    void updateProjectPermission(String project, String type, String permission, String name) throws Exception;

    //删除项目级访问权限
    void deleteProjectPermission(String project, String type, String name) throws Exception;

    //获取表行列级权限
    List<AclTCRResponse> getAclList(String type, String name, String project, boolean authorized_only) throws Exception;

    //更新表行列级权限
    void updateAcl(String type, String name, String project, AclTCRRequest request) throws Exception;
}
