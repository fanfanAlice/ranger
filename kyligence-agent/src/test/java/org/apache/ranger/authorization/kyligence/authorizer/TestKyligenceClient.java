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

package org.apache.ranger.authorization.kyligence.authorizer;

import com.google.common.collect.Maps;
import org.apache.ranger.services.kyligence.client.KyligenceClient;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

@Ignore
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
