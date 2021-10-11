package org.apache.ranger.authorization.kyligence.authorizer;

import org.apache.ranger.plugin.service.RangerBasePlugin;

public class RangerKyligencPlugin extends RangerBasePlugin {

    public RangerKyligencPlugin(String appId) {
        super("kyligence", appId);
    }

    @Override
    public void init() {
        super.init();
    }
}
