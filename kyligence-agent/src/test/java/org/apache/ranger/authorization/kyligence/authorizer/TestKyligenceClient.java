package org.apache.ranger.authorization.kyligence.authorizer;

import com.google.common.collect.Maps;
import org.apache.ranger.services.kyligence.client.KyligenceClient;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestKyligenceClient {

    @Test
    public void testGetProjectList() {
        Map<String, String> configs = Maps.newConcurrentMap();
        configs.put("kyligence.url", "http://10.1.2.181:7079");
        configs.put("username", "admin");
        configs.put("password", "test@1234");
        try {
            Map<String, Object> kyligence = KyligenceClient.connectionTest("kyligence", configs);
            System.out.println(kyligence);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
