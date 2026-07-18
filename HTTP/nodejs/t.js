'use strict';
/**
 * tra.js — Fixed 1.8.9 (protocol 47) login/keep-alive bridge.
 * 
 * Corrected S08PacketPlayerPosLook length (removed extra Teleport ID byte for 1.8.9).
 */

const net = require('net');
const http = require('http');
const fs = require('fs');
const path = require('path');

const GITHUB_PAGE_URL = 'https://flexi7pro.github.io/HtmlServer-mc/';
const GITHUB_REFETCH_INTERVAL_MS = 60000;

let remoteWorldInitBuffer = null;

// ---------------------------------------------------------------------------
// Helper primitives
// ---------------------------------------------------------------------------

function writeVarInt(value) {
  const out = [];
  value = value >>> 0;
  do {
    let temp = value & 0b01111111;
    value >>>= 7;
    if (value !== 0) temp |= 0b10000000;
    out.push(temp);
  } while (value !== 0);
  return Buffer.from(out);
}

function readVarInt(buf, offset) {
  let numRead = 0;
  let result = 0;
  let pos = offset;
  while (true) {
    if (pos >= buf.length) return null;
    const byte = buf[pos];
    pos++;
    result |= (byte & 0b01111111) << (7 * numRead);
    numRead++;
    if (numRead > 5) throw new Error('VarInt is too big');
    if ((byte & 0b10000000) === 0) break;
  }
  return { value: result | 0, length: pos - offset };
}

function writeString(str) {
  const strBuf = Buffer.from(str, 'utf8');
  return Buffer.concat([writeVarInt(strBuf.length), strBuf]);
}

function readString(buf, offset) {
  const lenInfo = readVarInt(buf, offset);
  if (!lenInfo) return null;
  const start = offset + lenInfo.length;
  const end = start + lenInfo.value;
  if (end > buf.length) return null;
  return { value: buf.slice(start, end).toString('utf8'), length: end - offset };
}

function writeInt8(n) { const b = Buffer.alloc(1); b.writeInt8(n, 0); return b; }
function writeUInt16(n) { const b = Buffer.alloc(2); b.writeUInt16BE(n, 0); return b; }
function writeInt32(n) { const b = Buffer.alloc(4); b.writeInt32BE(n, 0); return b; }
function writeFloat(n) { const b = Buffer.alloc(4); b.writeFloatBE(n, 0); return b; }
function writeDouble(n) { const b = Buffer.alloc(8); b.writeDoubleBE(n, 0); return b; }

function buildPacket(packetId, dataBuf) {
  const body = Buffer.concat([writeVarInt(packetId), dataBuf]);
  return Buffer.concat([writeVarInt(body.length), body]);
}

// ---------------------------------------------------------------------------
// Chunk Packet
// ---------------------------------------------------------------------------
function buildFlatChunkColumn() {
  const data = Buffer.alloc(0); 

  return Buffer.concat([
    writeInt32(0),                // Chunk X (4 bytes)
    writeInt32(0),                // Chunk Z (4 bytes)
    Buffer.from([0x01]),          // Ground-Up Continuous = True (1 byte)
    writeUInt16(0x00),            // Primary Bit Mask (2 bytes)
    writeVarInt(data.length),     // Size = 0 (1 byte VarInt)
    data,                         // 0 bytes
  ]);
}

// ---------------------------------------------------------------------------
// Packet Constructors
// ---------------------------------------------------------------------------

function packetLoginSuccess(uuidStr, username) {
  const data = Buffer.concat([writeString(uuidStr), writeString(username)]);
  return buildPacket(0x02, data); 
}

function packetJoinGame() {
  const data = Buffer.concat([
    writeInt32(1),            // Entity ID
    writeInt8(1),             // Gamemode: Creative
    writeInt8(0),             // Dimension: Overworld
    writeInt8(0),             // Difficulty: Peaceful
    writeInt8(1),             // Max players
    writeString('flat'),      // Level type
    Buffer.from([0x00]),      // Reduced debug info: false
  ]);
  return buildPacket(0x01, data); 
}

function packetChunkData() {
  return buildPacket(0x21, buildFlatChunkColumn()); 
}

function packetPlayerPositionAndLook() {
  const data = Buffer.concat([
    writeDouble(8.5), writeDouble(64.0), writeDouble(8.5), // X, Y, Z (24 bytes)
    writeFloat(0), writeFloat(0),                         // Yaw, Pitch (8 bytes)
    writeInt8(0)                                          // Flags (1 byte)
  ]);
  return buildPacket(0x08, data); 
}

function packetKeepAlive(id) {
  return buildPacket(0x00, writeVarInt(id));
}

function packetStatusResponse(motd, online, max) {
  const json = JSON.stringify({
    version: { name: '1.8.9', protocol: 47 },
    players: { max, online, sample: [] },
    description: { text: motd },
  });
  return buildPacket(0x00, writeString(json)); 
}

function packetPongResponse(payloadBuf) {
  return buildPacket(0x01, payloadBuf); 
}

// ---------------------------------------------------------------------------
// TCP Server
// ---------------------------------------------------------------------------

const PORT = process.env.TRA_PORT ? parseInt(process.env.TRA_PORT, 10) : 25565;
const KEEP_ALIVE_INTERVAL_MS = 15000;
const MAX_PACKET_LENGTH = 2097151; 

const STATE = { HANDSHAKE: 0, STATUS: 1, LOGIN: 2, PLAY: 3 };

const server = net.createServer((socket) => {
  socket.setNoDelay(true);

  let inbound = Buffer.alloc(0);
  let state = STATE.HANDSHAKE;
  let keepAliveTimer = null;
  let pendingKeepAliveId = null;
  let username = null;

  const send = (buf) => { if (!socket.destroyed) socket.write(buf); };

  const startKeepAlive = () => {
    keepAliveTimer = setInterval(() => {
      pendingKeepAliveId = Math.floor(Math.random() * 127);
      send(packetKeepAlive(pendingKeepAliveId));
    }, KEEP_ALIVE_INTERVAL_MS);
  };

  socket.on('data', (chunk) => {
    inbound = Buffer.concat([inbound, chunk]);

    while (true) {
      let lenInfo;
      try {
        lenInfo = readVarInt(inbound, 0);
      } catch (err) {
        socket.destroy();
        return;
      }
      if (!lenInfo) break; 

      const packetLength = lenInfo.value;
      if (packetLength < 0 || packetLength > MAX_PACKET_LENGTH) {
        socket.destroy();
        return;
      }

      const totalLength = lenInfo.length + packetLength;
      if (inbound.length < totalLength) break; 

      const packetBody = inbound.slice(lenInfo.length, totalLength);
      inbound = inbound.slice(totalLength);

      try {
        handlePacket(packetBody);
      } catch (err) {
        socket.destroy();
        return;
      }
    }
  });

  function handlePacket(body) {
    const idInfo = readVarInt(body, 0);
    if (!idInfo) throw new Error('Missing packet ID');
    const packetId = idInfo.value;
    const dataOffset = idInfo.length;

    if (state === STATE.HANDSHAKE && packetId === 0x00) {
      let off = dataOffset;
      const proto = readVarInt(body, off); off += proto.length;
      const addr = readString(body, off); off += addr.length;
      off += 2; 
      const nextState = readVarInt(body, off); 
      state = nextState.value; 
      return;
    }

    if (state === STATE.STATUS) {
      if (packetId === 0x00) {
        send(packetStatusResponse('§a§lسيرفر البلوكة الواحدة 🌳', 1, 100));
      } else if (packetId === 0x01) {
        const payload = body.slice(dataOffset, dataOffset + 8);
        send(packetPongResponse(payload));
      }
      return;
    }

    if (state === STATE.LOGIN && packetId === 0x00) {
      const nameInfo = readString(body, dataOffset);
      username = (nameInfo && nameInfo.value) ? nameInfo.value : 'Player';
      
      const crypto = require('crypto');
      const hash = crypto.createHash('md5').update('OfflinePlayer:' + username).digest();
      hash[6] = (hash[6] & 0x0f) | 0x30; 
      hash[8] = (hash[8] & 0x3f) | 0x80; 
      const hex = hash.toString('hex');
      const offlineUuid = [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-');

      send(packetLoginSuccess(offlineUuid, username));
      state = STATE.PLAY;
      console.log(`[tra.js] 🔑 اللاعب ${username} دخل السيرفر.`);

      send(packetJoinGame());

      setTimeout(() => {
        send(packetChunkData());
        console.log('[tra.js] 📦 تم إرسال الـ Chunk بنجاح.');
      }, 100);

      setTimeout(() => {
        send(packetPlayerPositionAndLook());
        console.log('[tra.js] 📍 تم تثبيت اللاعب في الفراغ.');
        startKeepAlive();
      }, 200);

      return;
    }

    if (state === STATE.PLAY) {
      if (packetId === 0x00) {
        const idResp = readVarInt(body, dataOffset);
        if (!idResp) throw new Error('Malformed keep-alive payload');
      }
      return;
    }
  }

  socket.on('close', () => { if (keepAliveTimer) clearInterval(keepAliveTimer); });
  socket.on('error', () => { if (keepAliveTimer) clearInterval(keepAliveTimer); });
});

server.listen(PORT, () => {
  console.log(`[tra.js] 🎮 المترجم يعمل على المنفذ: ${PORT}`);
});
