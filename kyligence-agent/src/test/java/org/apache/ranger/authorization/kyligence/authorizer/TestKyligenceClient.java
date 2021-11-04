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
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.kyligence.client.IClient;
import org.apache.ranger.services.kyligence.client.KyligenceClient;
import org.apache.ranger.services.kyligence.client.impl.KEClientImpl;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Ignore
public class TestKyligenceClient {

    @Test
    public void testGetProjectList() {
        Map<String, String> configs = Maps.newConcurrentMap();
        configs.put("kyligence.url", "http://10.1.2.181:7097");
        configs.put("username", "admin");
        configs.put("password", "test@1234");
        try {
            Map<String, Object> kyligence = KyligenceClient.connectionTest("kyligence", configs);
            System.out.println(kyligence);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Test
    public void testDecryptPassword() {
        try {
            String s = PasswordUtils.decryptPassword("PBEWithHmacSHA512AndAES_128,tzL1AKl5uc4NKYaoQ4P3WLGIBFPXWPWdu1fRm9004jtQiV,f77aLYLo,1000,/5y3uewCS62vFXBJnfV4ZA==,j896YSr8T0f14bbk0EzM1w==");
            System.out.println(s);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Test
    public void testGetDatabases() {
        IClient client = new KEClientImpl("http://10.1.2.181:7097", "admin", "test@1234");
        try {
            List<String> databases = client.getDatabases("test_gbase", "t");
            System.out.println(databases);
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
