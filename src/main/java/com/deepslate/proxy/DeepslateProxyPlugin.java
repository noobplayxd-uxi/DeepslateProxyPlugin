package com.deepslate.proxy;

import net.md_5.bungee.api.event.ProxyPingEvent;
import net.md_5.bungee.api.plugin.*;
import net.md_5.bungee.event.EventHandler;

public class DeepslateProxyPlugin extends Plugin implements Listener {

    @Override
    public void onEnable() {
        getProxy().getPluginManager().registerListener(this, this);
        getLogger().info("DeepslateProxyPlugin enabled – ready for future chunk handling");
    }

    @EventHandler
    public void onPing(ProxyPingEvent event) {
        // Placeholder – this event is just to ensure the plugin compiles and loads.
        // We will replace this with actual chunk packet interception later.
    }
}
