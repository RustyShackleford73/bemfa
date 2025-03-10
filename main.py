#!/bin/env python3

import asyncio
import json
import logging
import os
import urllib.parse
import subprocess
from typing import Dict


class bemfaTcpAPI:

    def __init__(self, host: str, port: int, api_key: str, devices: Dict, keep_alive_interval=60):
        self.host = host
        self.port = port
        self.api_key = api_key
        self.devices = devices
        self.keep_alive_interval = keep_alive_interval
        
        self.reader = None
        self.writer = None
        self.keepalive_task = None
        self.is_connected = False  # 标记当前连接状态

    async def connect(self):
        while True:
            try:
                # 尝试建立连接
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
                self.is_connected = True
                logging.info("Connected to server.")

                # 发送订阅消息
                messages = ["cmd=1&uid={uid}&topic={topic}\r\n".format(uid=self.api_key, topic=topic) for topic in self.devices.keys()]
                for message in messages:
                    self.writer.write(message.encode())
                    await self.writer.drain()

                # 启动保活任务
                self.keepalive_task = asyncio.create_task(self.keepalive())

                # 监听服务器消息
                while True:
                    try:
                        line = await self.reader.readline()
                        if not line:
                            break
                        line = line.decode().strip()
                        logging.info("Message incoming: %s", line)
                        qs = urllib.parse.parse_qs(line)
                        if qs['cmd'][0] == '0':  # ping echo reply
                            continue
                        yield qs
                    except ConnectionResetError:
                        logging.error("Connection reset by peer. Reconnecting...")
                        break  # 退出内部循环，重新连接

            except (ConnectionError, asyncio.TimeoutError, ConnectionResetError) as e:
                logging.error("Connection error: %s. Retrying in 10 seconds...", e)
            except Exception as e:
                logging.error("Unexpected error: %s", e)
                break
            finally:
                # 清理资源
                if self.writer:
                    self.writer.close()
                    try:
                        await self.writer.wait_closed()
                    except Exception as e:
                        logging.error("Error closing writer: %s", e)
                self.writer = None
                self.reader = None
                self.is_connected = False
                if self.keepalive_task:
                    self.keepalive_task.cancel()  # 取消保活任务

            await asyncio.sleep(10)  # 等待10秒后重试

    async def keepalive(self):
        while self.is_connected and self.writer:
            try:
                self.writer.write('ping\r\n'.encode())
                await self.writer.drain()
                await asyncio.sleep(self.keep_alive_interval)
            except ConnectionError:
                logging.error("Keepalive failed. Connection lost.")
                self.is_connected = False
                break

async def exec_command(command, state):
    try:
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        logging.info("Suspend: stdout: %s", stdout)
        logging.info("Suspend: stderr: %s", stderr)
    except Exception as e:
        logging.error("Error executing command %s: %s", command, e)

async def start_server(api):
    while True:
        try:
            async for message in api.connect():
                if 'topic' in message:
                    device = message['topic'][0]
                    if 'msg' in message:
                        print(api.devices[device][message['msg'][0]])
                        print(type(api.devices[device][message['msg'][0]]))
                        await exec_command(api.devices[device][message['msg'][0]], state=message['msg'][0])
        except Exception as e:
            logging.error("Error in start_server: %s. Retrying in 10 seconds...", e)
            await asyncio.sleep(10)  # 等待10秒后重试


def load_devices_from_json(devices_dir: str) -> Dict[str, Dict]:
    """
    从 devices 文件夹中加载所有设备的 JSON 配置文件。
    :param devices_dir: devices 文件夹路径
    :return: 设备配置字典，键为设备名称，值为设备配置
    """
    devices = {}
    for filename in os.listdir(devices_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(devices_dir, filename)
            with open(filepath, "r") as file:
                device_config = json.load(file)
                devices[device_config["name"]] = device_config
    return devices

def load_bemfa_cfg(cfg: str) -> Dict[str, Dict]:
    with open(cfg, "r") as file:
        bemfa_cfg = json.load(file)
    return bemfa_cfg

def main():
    cfg = load_bemfa_cfg('config.json')
    devices_dir = cfg['devices_dir']
    api_key = cfg['api_key'] 
    devices = load_devices_from_json(devices_dir)
    print(devices)
    api = bemfaTcpAPI('bemfa.com', '8344', api_key, devices)
    logging.basicConfig(level=logging.INFO)

    asyncio.run(start_server(api))


if __name__ == '__main__':
    main()