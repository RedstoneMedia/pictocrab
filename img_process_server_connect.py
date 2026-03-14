import os.path
from pathlib import Path

import cv2
import numpy as np
import subprocess
import struct
import socket
from threading import Thread
import time

if os.name == "nt":
    import win32pipe, win32file, pywintypes


SERVER_PIPE_NAME = "img_process_server.sock"
READ_BUFFER_SIZE = 8192
READ_TIMEOUT = 60
MAX_RETRY = 3


class CrossPlatformConnection:
    def __init__(self, name: str):
        self.name = name
        self.handle = None
        self.sock = None
        self.is_windows = os.name == "nt"

    def connect(self):
        if self.is_windows:
            while True:
                try:
                    handle = win32file.CreateFile(
                        fr"\\.\pipe\{self.name}",
                        win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                        0,
                        None,
                        win32file.OPEN_EXISTING,
                        0,
                        None
                    )
                    win32pipe.SetNamedPipeHandleState(handle, win32pipe.PIPE_READMODE_MESSAGE, None, None)
                    self.handle = handle
                    return
                except pywintypes.error as e:
                    if e.args[0] == 2:
                        print("no pipe, trying again in a sec")
                        time.sleep(1)
                    elif e.args[0] == 109:
                        raise Exception("broken pipe, bye bye")
        else:
            addr = b"\0" + self.name.encode()
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            while True:
                try:
                    self.sock.connect(addr)
                    return
                except (FileNotFoundError, ConnectionRefusedError):
                    time.sleep(1)

    def close(self):
        if os.name == "nt" and self.handle:
            win32file.CloseHandle(self.handle)
            self.handle = None
        elif not self.is_windows and self.sock:
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
            self.sock = None

    def write(self, data: bytes):
        msg_length_bytes = struct.pack(">I", len(data))
        if self.is_windows:
            try:
                win32file.WriteFile(self.handle, msg_length_bytes)
                win32file.WriteFile(self.handle, data)
            except pywintypes.error as e:
                if e.args[0] in (2, 109):
                    raise Exception("broken or missing pipe")
        else:
            self.sock.sendall(msg_length_bytes + data)

    def read(self, buffer_size: int) -> bytearray:
        if self.is_windows:
            try:
                msg_length_bytes = win32file.ReadFile(self.handle, 4)[1]
                msg_length = int.from_bytes(msg_length_bytes, "big")
                msg = bytearray()
                while len(msg) < msg_length:
                    remaining = msg_length - len(msg)
                    resp = win32file.ReadFile(self.handle, min(buffer_size, remaining))[1]
                    msg.extend(resp)
                return msg
            except pywintypes.error as e:
                if e.args[0] in (2, 109):
                    raise Exception("broken or missing pipe")
                raise
        else:
            msg_length_bytes = self._sock_recv_exact(4)
            msg_length = int.from_bytes(msg_length_bytes, "big")
            return self._sock_recv_exact(msg_length)

    def _sock_recv_exact(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise Exception("socket closed")
            buf.extend(chunk)
        return bytes(buf)

    def __del__(self):
        self.close()


class ImageProcessServerConnect:

    def __init__(self, cache_dir: str, threaded_reads: bool, working_dir = "./", fill_strategy = "reflect", filter_type = "Lanczos3", grayscale = False):
        #subprocess.Popen(["picto-crab"]) # Needs to be in path
        self.conn = CrossPlatformConnection(SERVER_PIPE_NAME)
        self.conn.connect()
        self.working_dir = working_dir
        self.grayscale = grayscale

        abs_cache_dir = str(Path(cache_dir).absolute())
        abs_work_dir = str(Path(working_dir).absolute())
        self.send_command("setup", [
            abs_cache_dir,
            abs_work_dir,
            str(threaded_reads).lower(),
            fill_strategy,
            filter_type,
            str(grayscale).lower()
        ])
        self.current_command = None

    def send_command(self, command_type : str, args : list):
        command_string = command_type + "|" + "|".join([str(arg) for arg in args])
        self.current_command = command_string
        self.conn.write(command_string.encode(encoding="utf-8"))


    def _ask_for_images(self, paths : list, width : int, height : int, output_images: list):
        self.send_command("gets", [width, height] + paths)
        for _ in paths:
            data = self.conn.read(READ_BUFFER_SIZE)
            image = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_GRAYSCALE if self.grayscale else cv2.IMREAD_COLOR)
            output_images.append(image)
        self.current_command = None

    def ask_for_images(self, paths : list, width : int, height : int) -> list:
        while self.current_command is not None:
            time.sleep(0.1)
        output_images = []
        for _ in range(0, MAX_RETRY):
            thread = Thread(target=self._ask_for_images, args=(paths, width, height, output_images))
            thread.start()
            thread.join(timeout=READ_TIMEOUT)
            if thread.is_alive():
                print("Image request timed out. Trying again")
                try:
                    self.conn.close()
                except OSError:
                    pass
                time.sleep(0.5)
                self.conn.connect()
                output_images.clear()
                continue
            break
        else:
            raise TimeoutError("Could not get images. Max retries reached")
        self.current_command = None
        return output_images


def benchmark(imgs_path: str):
    server = ImageProcessServerConnect("image_cache", False)
    img_paths = [os.path.join(imgs_path, name) for name in
                 os.listdir(imgs_path)]
    print(len(img_paths))
    # Prevent cold start
    start = time.time()
    imgs = server.ask_for_images(img_paths, width=128, height=128)
    assert len(imgs) == len(img_paths)
    print(f"Initial took {time.time()-start}s")

    iter_count = 20
    total_time = 0.0
    for i in range(iter_count):
        print(i)
        start = time.time()
        imgs = server.ask_for_images(img_paths, width=128, height=128)
        assert len(imgs) == len(img_paths)
        took = time.time() - start
        total_time += took
    avg_time = total_time/iter_count
    print(f"Took: {avg_time}s/iter, {avg_time / len(img_paths) * 1000}ms/image")


if __name__ == "__main__":
    server = ImageProcessServerConnect("image_cache", False)
    cv2.imshow("a", server.ask_for_images(["/home/mmustermann/Pictures/monte_pharaone.png"], 224, 224)[0])
    cv2.waitKey()