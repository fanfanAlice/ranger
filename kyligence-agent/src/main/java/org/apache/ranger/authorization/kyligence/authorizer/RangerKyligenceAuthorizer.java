package org.apache.ranger.authorization.kyligence.authorizer;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;
import java.util.Set;

import static java.util.Locale.ENGLISH;

public class RangerKyligenceAuthorizer {

    private static Logger LOG = LoggerFactory.getLogger(RangerKyligenceAuthorizer.class);

    final public static String RANGER_KYLIGENCE_SERVICETYPE = "kyligence";
    final public static String RANGER_KYLIGENCE_APPID = "kyligence";

    final private RangerBasePlugin rangerPlugin;

    public RangerKyligenceAuthorizer () {
        rangerPlugin = new RangerBasePlugin(RANGER_KYLIGENCE_SERVICETYPE, RANGER_KYLIGENCE_APPID);
        rangerPlugin.init();
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());
    }

    private RangerAccessResult getRowFilterResult(RangerKyligenceAccessRequest request) {
        if(LOG.isDebugEnabled()) {
            LOG.debug("==> getRowFilterResult(request=" + request + ")");
        }

        RangerAccessResult ret = rangerPlugin.evalRowFilterPolicies(request, null);

        if(LOG.isDebugEnabled()) {
            LOG.debug("<== getRowFilterResult(request=" + request + "): ret=" + ret);
        }

        return ret;
    }
}

class RangerKyligenceResource
        extends RangerAccessResourceImpl {


    public static final String KEY_PROJECT = "project";
    public static final String KEY_DATABASE = "database";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_FILTER_TYPE = "filtertype";

    public RangerKyligenceResource() {
    }

    public RangerKyligenceResource(String project, Optional<String> database, Optional<String> table) {
        setValue(KEY_PROJECT, project);
        if (database.isPresent()) {
            setValue(KEY_DATABASE, database.get());
        }
        if (table.isPresent()) {
            setValue(KEY_TABLE, table.get());
        }
    }

    public RangerKyligenceResource(String project, Optional<String> database, Optional<String> table, Optional<String> column) {
        setValue(KEY_PROJECT, project);
        if (database.isPresent()) {
            setValue(KEY_DATABASE, database.get());
        }
        if (table.isPresent()) {
            setValue(KEY_TABLE, table.get());
        }
        if (column.isPresent()) {
            setValue(KEY_COLUMN, column.get());
        }
    }
}

class RangerKyligenceAccessRequest
        extends RangerAccessRequestImpl {
    public RangerKyligenceAccessRequest(RangerKyligenceResource resource,
                                     String user,
                                     Set<String> userGroups,
                                        KyligenceAccessType kyligenceAccessType) {
        super(resource, kyligenceAccessType.name().toLowerCase(ENGLISH), user, userGroups, null);
        setAccessTime(new Date());
    }
}

enum KyligenceAccessType {
    SELECT, QUERY, OPERATION, MANAGEMENT, ADMIN, LIKE, IN, AND, OR
}
