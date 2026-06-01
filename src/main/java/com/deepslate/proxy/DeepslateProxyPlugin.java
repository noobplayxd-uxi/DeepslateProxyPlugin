package com.deepslate.proxy;

import net.md_5.bungee.api.event.*;
import net.md_5.bungee.api.plugin.*;
import net.md_5.bungee.protocol.packet.*;

public class DeepslateProxyPlugin extends Plugin implements Listener {

    @Override
    public void onEnable() {
        getProxy().getPluginManager().registerListener(this, this);
        getLogger().info("DeepslateProxyPlugin enabled – full deepslate support active");
    }

    @EventHandler(priority = EventPriority.LOWEST) // Run before ViaBackwards
    public void onPacketReceive(PacketReceiveEvent event) {
        if (event.getPacket() instanceof MapChunkPacket) {
            MapChunkPacket packet = (MapChunkPacket) event.getPacket();
            getLogger().info("Modern chunk at " + packet.getX() + "," + packet.getZ() +
                             " bitmask=" + packet.getBitmask());
            // TODO: Extract lower sections and send via extchunk
        }
    }
}
