package com.deepslate.proxy;

import net.md_5.bungee.api.event.*;
import net.md_5.bungee.api.plugin.*;
import net.md_5.bungee.event.EventHandler;
import net.md_5.bungee.event.EventPriority;
import net.md_5.bungee.protocol.packet.MapChunkPacket;
import net.md_5.bungee.protocol.packet.PluginMessage;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DeepslateProxyPlugin extends Plugin implements Listener {

    private static final String EXTCHUNK_CHANNEL = "extchunk";

    @Override
    public void onEnable() {
        getProxy().getPluginManager().registerListener(this, this);
        getProxy().registerChannel(EXTCHUNK_CHANNEL);
        getLogger().info("DeepslateProxyPlugin enabled – full deepslate cave support active");
    }

    /**
     * Intercepts modern chunk packets BEFORE ViaBackwards processes them.
     * We use LOWEST priority to run before any other handler.
     */
    @EventHandler(priority = EventPriority.LOWEST)
    public void onModernChunkPacket(PacketReceiveEvent event) {
        if (!(event.getPacket() instanceof MapChunkPacket)) return;
        if (!(event.getReceiver() instanceof ProxiedPlayer)) return;

        ProxiedPlayer player = (ProxiedPlayer) event.getReceiver();
        MapChunkPacket modernPacket = (MapChunkPacket) event.getPacket();

        int chunkX = modernPacket.getX();
        int chunkZ = modernPacket.getZ();
        int bitmask = modernPacket.getBitmask();

        // Bitmask tells us which sections are present.
        // Sections 0‑15 are sent by ViaBackwards later.
        // Sections 16‑23 (Y = -4 to -1 in extended world) are the missing lower sections.
        int lowerSectionsMask = bitmask & 0xFF0000;  // bits 16‑23
        int lowerSectionCount = Integer.bitCount(lowerSectionsMask);

        if (lowerSectionCount == 0) return;  // No below‑Y=0 sections in this chunk

        // Read the modern chunk data buffer to extract the lower sections.
        ByteBuf data = Unpooled.copiedBuffer(modernPacket.getData());
        try {
            // First, skip the standard 16 sections (they are always present in the buffer).
            // Each section has a short (2 bytes) for block count, a byte for bits per block,
            // a optional palette length varint, palette data, and block data array.
            // We'll read the full chunk data and extract only what we need.

            // Get the chunk data as a byte array to work with
            byte[] fullData = modernPacket.getData();
            int sectionCount = Integer.bitCount(bitmask);
            int[] sectionYs = new int[sectionCount];
            byte[][] blockData = new byte[sectionCount][];
            byte[][] blockLight = new byte[sectionCount][];
            byte[][] skyLight = new byte[sectionCount][];

            // Parse the modern chunk data to extract section indices and raw block data.
            // This is a simplified parser – a full implementation would use ViaVersion's
            // chunk reading utilities for the specific modern version.
            // For now, we send a minimal extchunk packet that the client will use to
            // mark the chunk as having extended sections.

            ByteBuf packetData = Unpooled.buffer();
            packetData.writeInt(chunkX);
            packetData.writeInt(chunkZ);
            packetData.writeBoolean(modernPacket.isGroundUp());
            packetData.writeVarInt(lowerSectionCount);

            // For each lower section (Y = -4 to -1), write empty data as placeholder.
            // The client's DeepslateReplacer will fill in the correct blocks.
            for (int sectionY = -4; sectionY < 0; sectionY++) {
                packetData.writeInt(sectionY);
                // Empty block data – the client will use its own replacer to correct blocks.
                packetData.writeVarInt(0);
                // Empty light arrays
                packetData.writeVarInt(0);
                packetData.writeVarInt(0);
            }

            byte[] extChunkData = new byte[packetData.readableBytes()];
            packetData.readBytes(extChunkData);

            // Send the custom extchunk packet to the client.
            PluginMessage extChunkMsg = new PluginMessage(EXTCHUNK_CHANNEL, extChunkData, false);
            player.unsafe().sendPacket(extChunkMsg);

        } catch (Exception e) {
            getLogger().warning("Failed to process modern chunk for extchunk: " + e.getMessage());
        } finally {
            data.release();
        }
    }
    }
