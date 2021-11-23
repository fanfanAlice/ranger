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

package org.apache.ranger.services.kyligence;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.PasswordUtils;
import org.apache.ranger.services.kyligence.client.KyligenceResourceMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RangerServiceKyligence extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceKyligence.class);

    public static final String ACCESS_TYPE_SELECT = "select";

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceKyligence.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        /*for (RangerPolicy defaultPolicy : ret) {
            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
                List<RangerPolicy.RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
                accessListForLookupUser.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
                RangerPolicy.RangerPolicyItem policyItemForLookupUser = new RangerPolicy.RangerPolicyItem();
                policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
                policyItemForLookupUser.setAccesses(accessListForLookupUser);
                policyItemForLookupUser.setDelegateAdmin(false);
                defaultPolicy.getPolicyItems().add(policyItemForLookupUser);
            }
        }*/

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceKyligence.getDefaultRangerPolicies()");
        }
        return ret;
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<>();
        String serviceName = getServiceName();
        if (null != configs.get(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
            configs.put("kyligence.password", configs.get(HadoopConfigHolder.RANGER_LOGIN_PASSWORD));
        }
        if (configs != null) {
            try {
                ret = KyligenceResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.info("<== RangerServiceKyligence.validateConfig Error:" + e);
                throw e;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.info("<== RangerServiceKyligence.validateConfig Response : (" + ret + " )");
        }
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<>();
        String serviceName = getServiceName();
        String serviceType = getServiceType();
        if (null != configs.get(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
            configs.put("kyligence.password", PasswordUtils.decryptPassword(configs.get(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)));
        }
        if (context != null) {
            try {
                ret = KyligenceResourceMgr.getKylinResources(serviceName, serviceType, configs, context);
            } catch (Exception e) {
                LOG.info("<==RangerServiceKyligence.lookupResource() Error : " + e);
                throw e;
            }
        }
        return ret;
    }
}
