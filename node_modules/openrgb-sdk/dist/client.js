"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const events_1 = __importDefault(require("events"));
const net_1 = require("net");
// bufferpack does not have type declarations
// @ts-expect-error
const bufferpack_1 = __importDefault(require("bufferpack"));
const utils = __importStar(require("./utils.js"));
const device_1 = __importDefault(require("./device"));
const HEADER_SIZE = 16;
const CLIENT_PROTOCOL_VERSION = 5;
class Client extends events_1.default {
    name;
    port;
    host;
    isConnected;
    protocolVersion;
    settings;
    resolver;
    currentPacketLength;
    socket;
    /**
     * @param {string} [name="nodejs"] name for the client
     * @param {number} [port=6742] port of the connection
     * @param {string} [host="127.0.0.1"] host of the connection
     * @param {Settings} [settings] settings for the connection
     */
    constructor(name, port, host, settings = {}) {
        super();
        this.name = name || "nodejs";
        this.port = +port || 6742;
        this.host = host || "127.0.0.1";
        if (this.host == "localhost")
            this.host = "127.0.0.1"; // node v17 doesn't acccept localhost anymore (bug?)
        this.isConnected = false;
        this.resolver = [];
        this.currentPacketLength = 0;
        this.settings = settings;
    }
    /**
     * connect to the OpenRGB-SDK-server
     */
    async connect(timeout = 1000) {
        this.socket = new net_1.Socket();
        let connectionPromise = Promise.race([
            new Promise((resolve) => this.socket.once("connect", resolve)),
            new Promise((resolve) => this.socket.once("error", resolve)),
            new Promise((resolve) => setTimeout(() => resolve("timeout"), timeout))
        ]);
        this.socket.connect(this.port, this.host);
        let res = await connectionPromise;
        if (res == "timeout")
            throw new Error("timeout");
        // todo???
        if (typeof res == "object")
            throw res;
        this.socket.on("close", () => {
            this.emit("disconnect");
            this.isConnected = false;
        });
        this.socket.on("error", (err) => {
            this.emit("error", err);
        });
        this.isConnected = true;
        this.currentPacketLength = 0;
        this.socket.on("readable", () => {
            // sometimes several packets only emit one event
            // loop until there are no more headers
            while (true) {
                // if the packet couldn't be read the first time (the reason why that could happen is described in the next block),
                // the packet length will be taken over in to the next `readable call` to then try and read the whole package
                if (this.currentPacketLength == 0) {
                    let header = this.socket.read(HEADER_SIZE);
                    if (!header)
                        return;
                    // check for package magic "ORGB"
                    if (!(header.slice(0, 4).equals(Buffer.from([0x4f, 0x52, 0x47, 0x42]))))
                        return;
                    this.currentPacketLength = header.readUInt32LE(12);
                    if (this.currentPacketLength > 0) {
                        if (this.resolver[0]) {
                            this.resolver[0].header = header;
                        }
                    }
                    else {
                        // some packets have no body, only header
                        if (this.resolver.length) {
                            resolve.bind(this)(header);
                        }
                    }
                }
                if (this.currentPacketLength > 0) {
                    // if the packets size is bigger than what the data currently available on the socket res will be null
                    // this means, that the packet has been split in two (or potentially even more chunks)
                    // the code waits for the next `readable` event so the rest of the data is collected
                    let res = this.socket.read(this.currentPacketLength);
                    if (!res)
                        return;
                    this.currentPacketLength = 0;
                    if (this.resolver[0]) {
                        resolve.bind(this)(Buffer.concat([this.resolver[0].header, res]));
                    }
                }
            }
        });
        let serverProtocolVersion = await new Promise(async (resolve, reject) => {
            setTimeout(() => reject(0), timeout);
            resolve(await this.getProtocolVersion());
        }).catch(_ => _);
        if (this.settings.forceProtocolVersion && serverProtocolVersion == 0) {
            this.protocolVersion = this.settings.forceProtocolVersion;
        }
        else {
            let clientVersion = ("forceProtocolVersion" in this.settings) ? this.settings.forceProtocolVersion : CLIENT_PROTOCOL_VERSION;
            this.protocolVersion = (serverProtocolVersion < clientVersion) ? serverProtocolVersion : clientVersion;
        }
        this.setClientName(this.name);
        this.emit("connect");
    }
    /**
     * disconnect from the OpenRGB-SDK-server
     */
    disconnect() {
        if (this.isConnected) {
            this.socket.end();
            this.resolver = [];
        }
    }
    /**
     * get the amount of devices
     * @returns {Promise<number>}
     */
    async getControllerCount() {
        this.sendMessage(utils.command.requestControllerCount);
        const buffer = await this.readMessage(utils.command.requestControllerCount);
        return buffer.readUInt32LE();
    }
    /**
     * get the protocol version
     * @returns {Promise<number>}
     */
    async getProtocolVersion() {
        let clientVersion = ("forceProtocolVersion" in this.settings) ? this.settings.forceProtocolVersion : CLIENT_PROTOCOL_VERSION;
        this.sendMessage(utils.command.requestProtocolVersion, bufferpack_1.default.pack("<I", [clientVersion]));
        const buffer = await this.readMessage(utils.command.requestProtocolVersion);
        return buffer.readUInt32LE();
    }
    /**
     * get the properties of a device
     * @param {number} deviceId the id of the requested device
     * @returns {Promise<Device>}
     */
    async getControllerData(deviceId) {
        this.sendMessage(utils.command.requestControllerData, bufferpack_1.default.pack("<I", [this.protocolVersion]), deviceId);
        const buffer = await this.readMessage(utils.command.requestControllerData, deviceId);
        return new device_1.default(buffer, deviceId, this.protocolVersion);
    }
    /**
     * get the properties of all devices
     * @returns {Promise<Device[]>}
     */
    async getAllControllerData() {
        let devices = [];
        let controllerCount = await this.getControllerCount();
        for (let i = 0; i < controllerCount; i++) {
            devices.push(await this.getControllerData(i));
        }
        return devices;
    }
    /**
     * get a list of all profiles
     * @returns {Promise<String[]>}
     */
    async getProfileList() {
        this.sendMessage(utils.command.requestProfileList);
        const buffer = (await this.readMessage(utils.command.requestProfileList)).slice(4);
        let count = buffer.readUInt16LE();
        let offset = 2;
        let profiles = [];
        for (let i = 0; i < count; i++) {
            let length = buffer.readUInt16LE(offset);
            offset += 2;
            profiles.push(bufferpack_1.default.unpack(`<${length - 1}c`, buffer, offset).join(""));
            offset += length;
        }
        return profiles;
    }
    /**
     * request a rescan of devices in OpenRGB
     */
    requestRescan() {
        this.sendMessage(utils.command.requestRescanDevices);
    }
    /**
     * set the name of the client
     * @param {string} name the name displayed in openrgb
     */
    setClientName(name) {
        let nameBytes = Buffer.concat([new TextEncoder().encode(name), Buffer.from([0x00])]);
        this.sendMessage(utils.command.setClientName, nameBytes);
    }
    /**
     * update all leds of a device
     * @param {number} deviceId the id of the device
     * @param {RGBColor[]} colors the colors the device should be set to
     */
    updateLeds(deviceId, colors) {
        const size = 4 + 2 + (4 * colors.length);
        const colorsBuffer = Buffer.alloc(size - 4);
        colorsBuffer.writeUInt16LE(colors.length);
        for (let i = 0; i < colors.length; i++) {
            const color = colors[i];
            const offset = 2 + (i * 4);
            colorsBuffer.writeUInt8(color.red, offset);
            colorsBuffer.writeUInt8(color.green, offset + 1);
            colorsBuffer.writeUInt8(color.blue, offset + 2);
        }
        const prefixBuffer = Buffer.alloc(4);
        prefixBuffer.writeUInt32LE(size);
        this.sendMessage(utils.command.updateLeds, Buffer.concat([prefixBuffer, colorsBuffer]), deviceId);
    }
    /**
     * update all leds of a zone
     * @param {number} deviceId the id of the device
     * @param {number} zoneId the id of the zone
     * @param {RGBColor[]} colors the colors the zone should be set to
     */
    updateZoneLeds(deviceId, zoneId, colors) {
        const size = 4 + 6 + (4 * colors.length);
        const colorsBuffer = Buffer.alloc(size - 4);
        colorsBuffer.writeUInt32LE(zoneId);
        colorsBuffer.writeUInt16LE(colors.length, 4);
        for (let i = 0; i < colors.length; i++) {
            const color = colors[i];
            const offset = 6 + (i * 4);
            colorsBuffer.writeUInt8(color.red, offset);
            colorsBuffer.writeUInt8(color.green, offset + 1);
            colorsBuffer.writeUInt8(color.blue, offset + 2);
        }
        const prefixBuffer = Buffer.alloc(4);
        prefixBuffer.writeUInt32LE(size);
        this.sendMessage(utils.command.updateZoneLeds, Buffer.concat([prefixBuffer, colorsBuffer]), deviceId);
    }
    /**
     * update one led of a device
     * @param {number} deviceId the id of the device
     * @param {number} ledId the id of the led
     * @param {RGBColor} color the color the led should be set to
     */
    updateSingleLed(deviceId, ledId, color) {
        let buff = Buffer.concat([bufferpack_1.default.pack("<I", [ledId]), bufferpack_1.default.pack("<BBB", [color.red, color.green, color.blue]), Buffer.alloc(1)]);
        this.sendMessage(utils.command.updateSingleLed, buff, deviceId);
    }
    /**
     * sets the device to its mode for individual led control
     * @param {number} deviceId the id of the requested device
     */
    setCustomMode(deviceId) {
        this.sendMessage(utils.command.setCustomMode, undefined, deviceId);
    }
    /**
     * update the mode of a device
     * @param {number} deviceId the id of the device
     * @param {ModeInput} mode All fields are optional and missing ones will be filled in with the currently active settings.
     * Either id or name must be given as an indication for which mode should be set.
     * Purely informational fields like brightnessMax will be ignored but are allowed
     */
    async updateMode(deviceId, mode) {
        await sendMode.bind(this)(deviceId, mode, false);
    }
    /**
     * update the mode of a device and save it to the device
     * @param {number} deviceId the id of the device
     * @param {ModeInput} mode all fields are optional and missing ones will be filled in with the currently active settings
     * Either id or name must be given as an indication for which mode should be set.
     * Purely informational fields like brightnessMax will be ignored but are allowed
     */
    async saveMode(deviceId, mode) {
        await sendMode.bind(this)(deviceId, mode, true);
    }
    /**
     * resize a zone
     * @param {number} deviceId the id of the device
     * @param {number} zoneId the id of the zone
     * @param {number} zoneLength the length the zone should be set to
     */
    resizeZone(deviceId, zoneId, zoneLength) {
        this.sendMessage(utils.command.resizeZone, bufferpack_1.default.pack("<ii", [zoneId, zoneLength]), deviceId);
    }
    /**
     * create a new profile with the current state of the devices in openrgb
     * @param {string} name the name of the new profile
     */
    saveProfile(name) {
        let nameBytes = Buffer.concat([new TextEncoder().encode(name), Buffer.from([0x00])]);
        this.sendMessage(utils.command.saveProfile, nameBytes);
    }
    /**
     * load a profile out of the storage
     * @param {string} name the name of the profile that should be loaded
     */
    loadProfile(name) {
        let nameBytes = Buffer.concat([new TextEncoder().encode(name), Buffer.from([0x00])]);
        this.sendMessage(utils.command.loadProfile, nameBytes);
    }
    /**
     * delete a profile out of the storage
     * @param {string} name the name of the profile that should be deleted
     */
    deleteProfile(name) {
        let nameBytes = Buffer.concat([new TextEncoder().encode(name), Buffer.from([0x00])]);
        this.sendMessage(utils.command.deleteProfile, nameBytes);
    }
    /**
     * adds a segment to the specified Zone
     * @param deviceId id of the device
     * @param zoneId id of the zone
     * @param name name of the new segment
     * @param type zone type of the zone
     * @param start first led in the segment
     * @param length amount of leds in the segment
    */
    addSegment(deviceId, zoneId, name, type, start, length) {
        let size = 22 + name.length + 1;
        let buffer = Buffer.alloc(size);
        buffer.writeUInt32LE(size);
        buffer.writeUInt32LE(zoneId, 4);
        buffer.writeUInt16LE(name.length + 1, 8);
        buffer.write(name, 10);
        buffer.writeUInt8(0, 10 + name.length);
        buffer.writeInt32LE(type, 11 + name.length);
        buffer.writeUInt32LE(start, 15 + name.length);
        buffer.writeUInt32LE(length, 19 + name.length);
        this.sendMessage(utils.command.addSegment, buffer, deviceId);
    }
    /**
     * removes a segment
     * @param deviceId id of the device
     * @param zoneId id of the zone
     */
    clearSegments(deviceId, zoneId) {
        let buffer = Buffer.alloc(4);
        buffer.writeUint32LE(zoneId);
        this.sendMessage(utils.command.clearSegments, buffer, deviceId);
    }
    /**
     * @private
     */
    sendMessage(commandId, buffer = Buffer.alloc(0), deviceId = 0) {
        if (!this.isConnected)
            throw new Error("can't write to socket if not connected to OpenRGB");
        const header = this.encodeHeader(commandId, buffer.byteLength, deviceId);
        const packet = Buffer.concat([header, buffer]);
        this.socket.write(packet);
    }
    /**
     * @private
     */
    async readMessage(commandId, deviceId = 0) {
        if (!this.isConnected)
            throw new Error("can't read from socket if not connected to OpenRGB");
        return new Promise(resolve => this.resolver.push({ resolve, commandId, deviceId }));
    }
    /**
     * @private
     */
    encodeHeader(commandId, length, deviceId) {
        const buffer = Buffer.alloc(HEADER_SIZE);
        let index = buffer.write("ORGB", "ascii");
        index = buffer.writeUInt32LE(deviceId, index);
        index = buffer.writeUInt32LE(commandId, index);
        index = buffer.writeUInt32LE(length, index);
        return buffer;
    }
    /**
     * @private
     */
    decodeHeader(buffer) {
        const deviceId = buffer.readUInt32LE(4);
        const commandId = buffer.readUInt32LE(8);
        const length = buffer.readUInt32LE(12);
        return { deviceId, commandId, length };
    }
    /**
     * @private
     */
    pack_color_list(arr) {
        let out = bufferpack_1.default.pack("<H", [arr.length]);
        arr.forEach(element => {
            out = Buffer.concat([out, Buffer.from(""), bufferpack_1.default.pack("<BBBx", [element.red, element.green, element.blue])]);
        });
        return out;
    }
    /**
     * @private
     */
    pack_string(string) {
        return Buffer.concat([bufferpack_1.default.pack(`<H${string.length}s`, [string.length + 1, string]), Buffer.from('\x00')]);
    }
}
exports.default = Client;
async function sendMode(deviceId, mode, save) {
    //TODO: shorten and beautify
    if (typeof deviceId != "number")
        throw new Error("arg deviceId not given");
    let device = await this.getControllerData(deviceId);
    let modeId, modeName;
    switch (typeof mode) {
        case "number":
            modeId = mode;
            break;
        case "string":
            modeName = mode;
            break;
        case "object":
            if ("id" in mode)
                modeId = mode.id;
            else if ("name" in mode)
                modeName = mode.name;
            else
                throw new Error("Either mode.id or mode.name have to be given, but both are missing");
            break;
        default:
            throw new Error(`Mode must be of type number, string or object, but is of type ${typeof mode} `);
    }
    let modeData;
    if (modeId !== undefined) {
        if (!device.modes[modeId])
            throw new Error("Id given is not the id of a mode");
        modeData = device.modes[modeId];
    }
    else if (modeName !== undefined) {
        const nameSearch = device.modes.find((elem) => elem.name.toLowerCase() == modeName.toLowerCase());
        if (nameSearch === undefined)
            throw new Error("Name given is not the name of a mode");
        modeData = nameSearch;
    }
    else {
        // this can never be triggered, it just to shut ts up
        throw new Error(`Mode must be of type number, string or object, but is of type ${typeof mode} `);
    }
    if (typeof mode == "object") {
        if (mode.speed)
            modeData.speed = mode.speed;
        if (mode.brightness)
            modeData.brightness = mode.brightness;
        if (mode.direction)
            modeData.direction = mode.direction;
        if (mode.colorMode)
            modeData.colorMode = mode.colorMode;
        if (mode.colors)
            modeData.colors = mode.colors;
    }
    // auto send per-led
    // if (mode.colorMode) {
    // 	if (!modeData.flagList.includes("randomColor")) throw new Error("Random color can't be chosen")
    // 	modeData.colorMode = 3
    // } else if (modeData.flagList.includes("modeSpecificColor")) {
    // 	modeData.colorMode = 2
    // } else if (modeData.flagList.includes("perLedColor")) {
    // 	modeData.colorMode = 1
    // } else {
    // 	modeData.colorMode = 0
    // }
    // if (!isNaN(+mode.speed)) {
    // 	if (modeData.flagList.includes("speed")) {
    // 		if (mode.speed < modeData.speedMin || mode.speed > modeData.speedMax) throw new Error("Speed either too high or too low")
    // 		if ((mode.speed % 1) != 0) throw new Error("Speed must be a round number")
    // 		modeData.speed = mode.speed
    // 	} else throw new Error("Speed can't be set for this mode")
    // }
    // if (!isNaN(+mode.brightness)) {
    // 	if (modeData.flagList.includes("brightness")) {
    // 		if (mode.brightness < modeData.brightnessMin || mode.brightness > modeData.brightnessMax) throw new Error("Brightness either too high or too low")
    // 		if ((mode.brightness % 1) != 0) throw new Error("Brightness must be a round number")
    // 		modeData.brightness = mode.brightness
    // 	} else throw new Error("Brightness can't be set for this mode")
    // }
    // if (!isNaN(+mode.direction)) {
    // 	if (modeData.flagList.includes("direction")) {
    // 		let arr = ["directionLR", "directionUD", "directionHV"]
    // 		if (modeData.flagList.includes(arr[Math.floor(mode.direction / 2)])) {
    // 			modeData.direction = mode.direction
    // 		} else throw new Error("Can't set direction to this value")
    // 		if ((mode.direction % 1) != 0) throw new Error("Direction must be a round number")
    // 	} else throw new Error("Direction can't be set for this mode")
    // }
    // if (mode.colors) {
    // 	if (modeData.colorMode == 1) {
    // 		this.updateLeds(deviceId, mode.colors)
    // 	} else {
    // 		if (mode.colors.length > modeData.colorMax) throw new Error("Too many colors.")
    // 		if (modeData.colorLength <= 0) throw new Error("Color can't be set for this mode")
    // 		modeData.colors = mode.colors
    // 	}
    // }
    let pack;
    if (this.protocolVersion >= 3) {
        pack = bufferpack_1.default.pack("<12I", [
            modeData.value,
            modeData.flags,
            modeData.speedMin,
            modeData.speedMax,
            modeData.brightnessMin,
            modeData.brightnessMax,
            modeData.colorMin,
            modeData.colorMax,
            modeData.speed,
            modeData.brightness,
            modeData.direction,
            modeData.colorMode
        ]);
    }
    else {
        pack = bufferpack_1.default.pack("<9I", [
            modeData.value,
            modeData.flags,
            modeData.speedMin,
            modeData.speedMax,
            modeData.colorMin,
            modeData.colorMax,
            modeData.speed,
            modeData.direction,
            modeData.colorMode
        ]);
    }
    let data = Buffer.concat([
        bufferpack_1.default.pack("<I", [modeData.id]),
        this.pack_string(modeData.name),
        pack,
        this.pack_color_list(modeData.colors ? modeData.colors : []),
    ]);
    data = Buffer.concat([bufferpack_1.default.pack("<I", [data.length + bufferpack_1.default.calcLength("<I")]), data]);
    this.sendMessage(save ? utils.command.saveMode : utils.command.updateMode, data, deviceId);
}
function resolve(buffer) {
    let { deviceId, commandId } = this.decodeHeader(buffer);
    switch (commandId) {
        case utils.command.deviceListUpdated: {
            this.emit("deviceListUpdated");
            break;
        }
        default: {
            if (this.resolver.length) {
                let index = this.resolver.findIndex(resolver => resolver.deviceId == deviceId && resolver.commandId == commandId);
                if (index < 0)
                    return;
                this.resolver.splice(index, 1)[0].resolve(buffer.slice(16));
            }
        }
    }
}
