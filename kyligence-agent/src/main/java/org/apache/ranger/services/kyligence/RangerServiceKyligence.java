package org.apache.ranger.services.kyligence;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.client.HadoopConfigHolder;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.kyligence.client.KyligenceResourceMgr;

import java.util.*;

public class RangerServiceKyligence extends RangerBaseService {

    private static final Log LOG = LogFactory.getLog(RangerServiceKyligence.class);

    public static final String ACCESS_TYPE_SELECT = "select";

    @Override
    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceKyligence.getDefaultRangerPolicies()");
        }

        List<RangerPolicy> ret = super.getDefaultRangerPolicies();
        for (RangerPolicy defaultPolicy : ret) {
            if (defaultPolicy.getName().contains("all") && StringUtils.isNotBlank(lookUpUser)) {
                List<RangerPolicy.RangerPolicyItemAccess> accessListForLookupUser = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
                accessListForLookupUser.add(new RangerPolicy.RangerPolicyItemAccess(ACCESS_TYPE_SELECT));
                RangerPolicy.RangerPolicyItem policyItemForLookupUser = new RangerPolicy.RangerPolicyItem();
                policyItemForLookupUser.setUsers(Collections.singletonList(lookUpUser));
                policyItemForLookupUser.setAccesses(accessListForLookupUser);
                policyItemForLookupUser.setDelegateAdmin(false);
                defaultPolicy.getPolicyItems().add(policyItemForLookupUser);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceKyligence.getDefaultRangerPolicies()");
        }
        return ret;
    }

    @Override
    public Map<String, Object> validateConfig() throws Exception {
        Map<String, Object> ret = new HashMap<String, Object>();
        String serviceName = getServiceName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceKyligence.validateConfig Service: (" + serviceName + " )");
        }
        if (configs != null) {
            try {
                ret = KyligenceResourceMgr.validateConfig(serviceName, configs);
            } catch (Exception e) {
                LOG.error("<== RangerServiceKyligence.validateConfig Error:" + e);
                throw e;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceKyligence.validateConfig Response : (" + ret + " )");
        }
        return ret;
    }

    @Override
    public List<String> lookupResource(ResourceLookupContext context) throws Exception {
        List<String> ret = new ArrayList<String>();
        String serviceName = getServiceName();
        String serviceType = getServiceType();
        Map<String, String> configs = getConfigs();
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceKyligence.lookupResource() Context: (" + context + ")");
        }
        if (context != null) {
            try {
                if (!configs.containsKey(HadoopConfigHolder.RANGER_LOGIN_PASSWORD)) {
                    configs.put(HadoopConfigHolder.RANGER_LOGIN_PASSWORD, null);
                }
                ret = KyligenceResourceMgr.getKylinResources(serviceName, serviceType, configs, context);
            } catch (Exception e) {
                LOG.error("<==RangerServiceKyligence.lookupResource() Error : " + e);
                throw e;
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceKyligence.lookupResource() Response: (" + ret + ")");
        }
        return ret;
    }
}
