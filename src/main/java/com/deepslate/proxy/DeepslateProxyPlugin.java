package com.deepslate.proxy;

import com.viaversion.viaversion.api.Via;
import com.viaversion.viaversion.api.data.MappingData;
import com.viaversion.viaversion.api.protocol.Protocol;
import com.viaversion.viaversion.api.protocol.version.ProtocolVersion;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.connection.Server;
import net.md_5.bungee.api.event.ServerConnectedEvent;
import net.md_5.bungee.api.plugin.Listener;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.event.EventHandler;
import net.md_5.bungee.protocol.packet.PluginMessage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DeepslateProxyPlugin extends Plugin implements Listener {

    private static final String EXT_CHUNK_CHANNEL = "extchunk";

    @Override
    public void onEnable() {
        getProxy().getPluginManager().registerListener(this, this);
        getLogger().info("DeepslateProxyPlugin enabled – mapping deepslate via ViaVersion");
    }
//Container Events handling for  render
    @EventHandler
    public void onServerConnected(ServerConnectedEvent event) {
        ProxiedPlayer player = event.getPlayer();
        Server server = event.getServer();
        Channel serverChannel = server.getCh().getHandle();
        serverChannel.pipeline().addBefore("bungee-downstream-bridge", "deepslate-chunk-capture",
            new ChannelInboundHandlerAdapter() {
                @Override
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                    if (msg instanceof ByteBuf) {
                        ByteBuf buf = (ByteBuf) msg;
                        int readerIndex = buf.readerIndex();
                        try {
                            int packetId = readVarInt(buf);
                            if (packetId == 0x22) { // Chunk Data packet for 1.20.1 (PLAY)
                                buf.readerIndex(readerIndex);
                                ByteBuf copy = buf.copy();
                                try {
                                    processModernChunk(player, copy);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                } finally {
                                    copy.release();
                                }
                            }
                        } catch (Exception ignored) {}
                        buf.readerIndex(readerIndex);
                    }
                    super.channelRead(ctx, msg);
                }
            });
    }

    private void processModernChunk(ProxiedPlayer player, ByteBuf buf) throws IOException {
        readVarInt(buf); // skip packet id
        int chunkX = buf.readInt();
        int chunkZ = buf.readInt();
        boolean groundUp = buf.readBoolean();
        int bitmask = readVarInt(buf);

        // Skip heightmaps (NBT)
        skipNBT(buf);

        // Get protocol mapping 1.20.1 -> 1.12.2
        Protocol protocol = Via.getManager().getProtocolManager()
                .getProtocol(ProtocolVersion.v1_20_1, ProtocolVersion.v1_12_2);
        if (protocol == null) {
            getLogger().warning("No protocol mapping found for 1.20.1 -> 1.12.2");
            return;
        }
        MappingData mappingData = protocol.getMappingData();

        List<Integer> belowYList = new ArrayList<>();
        List<byte[]> belowBlockDataList = new ArrayList<>();
        List<byte[]> belowBlockLightList = new ArrayList<>();
        List<byte[]> belowSkyLightList = new ArrayList<>();

        for (int sectionY = -4; sectionY < 20; sectionY++) {
            if ((bitmask & (1 << (sectionY + 4))) != 0) {
                // Read paletted container
                short blockCount = buf.readShort();
                byte bitsPerBlock = buf.readByte();
                int paletteLength = readVarInt(buf);
                int[] palette = new int[paletteLength];
                for (int i = 0; i < paletteLength; i++) {
                    palette[i] = readVarInt(buf);
                }
                int dataArraySize = readVarInt(buf);
                long[] data = new long[dataArraySize];
                for (int i = 0; i < dataArraySize; i++) {
                    data[i] = buf.readLong();
                }

                // Convert to 1.12.2 char array using ViaVersion mapping
                byte[] blockData = convertSectionBlocks(mappingData, palette, data, bitsPerBlock);
                byte[] blockLight = new byte[2048];
                buf.readBytes(blockLight);
                byte[] skyLight = new byte[2048];
                buf.readBytes(skyLight);

                if (sectionY < 0) {
                    belowYList.add(sectionY);
                    belowBlockDataList.add(blockData);
                    belowBlockLightList.add(blockLight);
                    belowSkyLightList.add(skyLight);
                }
            }
        }

        if (belowYList.isEmpty()) return;

        // Build extchunk payload
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteOut);
        out.writeInt(chunkX);
        out.writeInt(chunkZ);
        out.writeBoolean(groundUp);
        writeVarInt(out, belowYList.size());
        for (int i = 0; i < belowYList.size(); i++) {
            out.writeInt(belowYList.get(i));
            writeVarInt(out, belowBlockDataList.get(i).length);
            out.write(belowBlockDataList.get(i));
            writeVarInt(out, belowBlockLightList.get(i).length);
            out.write(belowBlockLightList.get(i));
            writeVarInt(out, belowSkyLightList.get(i).length);
            out.write(belowSkyLightList.get(i));
        }
        out.flush();
        byte[] payload = byteOut.toByteArray();

        PluginMessage pluginMessage = new PluginMessage(EXT_CHUNK_CHANNEL, payload, false);
        player.unsafe().sendPacket(pluginMessage);
    }

    private byte[] convertSectionBlocks(MappingData mappingData, int[] palette, long[] data, int bitsPerBlock) {
        byte[] result = new byte[4096 * 2]; // 16*16*16 blocks, 2 bytes each (char)
        int[] blockIndices = new int[4096];
        // Unpack indices from long array
        if (bitsPerBlock > 0) {
            int idx = 0;
            int bitOffset = 0;
            for (long l : data) {
                for (int i = 0; i < 64 && idx < 4096; i += bitsPerBlock, bitOffset += bitsPerBlock) {
                    if (bitOffset + bitsPerBlock > 64) { bitOffset = 0; break; }
                    int paletteIdx = (int) ((l >> bitOffset) & ((1L << bitsPerBlock) - 1));
                    blockIndices[idx++] = paletteIdx;
                }
            }
        }
        // Translate each block
        for (int i = 0; i < 4096; i++) {
            int modernStateId = palette[blockIndices[i]];
            int oldState = mappingData.getOldBlockStateId(modernStateId);
            int blockId = oldState >> 4;
            int meta = oldState & 0xF;
            char b = (char) ((blockId & 0xFFF) << 4 | (meta & 0xF));
            result[i * 2] = (byte) (b >> 8);
            result[i * 2 + 1] = (byte) b;
        }
        return result;
    }

    // ===== VarInt and NBT helpers =====
    private static int readVarInt(ByteBuf buf) {
        int value = 0;
        int length = 0;
        byte current;
        do {
            current = buf.readByte();
            value |= (current & 0x7F) << (length * 7);
            length++;
            if (length > 5) throw new RuntimeException("VarInt too big");
        } while ((current & 0x80) == 0x80);
        return value;
    }

    private static void writeVarInt(DataOutputStream out, int value) throws IOException {
        do {
            byte temp = (byte) (value & 0x7F);
            value >>>= 7;
            if (value != 0) temp |= 0x80;
            out.writeByte(temp);
        } while (value != 0);
    }

    private void skipNBT(ByteBuf buf) {
        skipCompound(buf);
    }

    private void skipCompound(ByteBuf buf) {
        byte type;
        while ((type = buf.readByte()) != 0) { // 0 = TAG_End
            int nameLen = buf.readUnsignedShort();
            buf.skipBytes(nameLen);
            skipTagPayload(buf, type);
        }
    }

    private void skipTagPayload(ByteBuf buf, byte type) {
        switch (type) {
            case 1: // byte
                buf.readByte();
                break;
            case 2: // short
                buf.readShort();
                break;
            case 3: // int
                buf.readInt();
                break;
            case 4: // long
                buf.readLong();
                break;
            case 5: // float
                buf.readFloat();
                break;
            case 6: // double
                buf.readDouble();
                break;
            case 7: // byte array
                int len = buf.readInt();
                buf.skipBytes(len);
                break;
            case 8: // string
                int strLen = buf.readUnsignedShort();
                buf.skipBytes(strLen);
                break;
            case 9: // list
                byte listType = buf.readByte();
                int listLen = buf.readInt();
                for (int i = 0; i < listLen; i++) {
                    skipTagPayload(buf, listType);
                }
                break;
            case 10: // compound
                skipCompound(buf);
                break;
            case 11: // int array
                int intArrLen = buf.readInt();
                buf.skipBytes(intArrLen * 4);
                break;
            case 12: // long array
                int longArrLen = buf.readInt();
                buf.skipBytes(longArrLen * 8);
                break;
            default:
                throw new RuntimeException("Unknown NBT tag type: " + type);
        }
    }
}
