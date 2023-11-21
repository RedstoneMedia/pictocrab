import os.path
import cv2
import numpy as np
import subprocess
import struct
from threading import Thread
import time
import win32pipe, win32file, pywintypes

SERVER_PIPE_NAME = "img_process_server"
READ_BUFFER_SIZE = 4069
READ_TIMEOUT = 60
MAX_RETRY = 3

def connect_to_pipe(name: str):
    while True:
        try:
            handle = win32file.CreateFile(
                fr"\\.\pipe\{name}",
                win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                0,
                None,
                win32file.OPEN_EXISTING,
                0,
                None
            )
            res = win32pipe.SetNamedPipeHandleState(handle, win32pipe.PIPE_READMODE_MESSAGE, None, None)
            if res == 0:
                print(f"SetNamedPipeHandleState return code: {res}")
            return handle
        except pywintypes.error as e:
            if e.args[0] == 2:
                print("no pipe, trying again in a sec")
                time.sleep(1)
            elif e.args[0] == 109:
                raise Exception("broken pipe, bye bye")


def read_msg(handle, buffer_size) -> bytearray:
    try:
        while True:
            msg_length_bytes = win32file.ReadFile(handle, 4)[1]
            if msg_length_bytes == None:
                time.sleep(0.01)
                continue
            msg_length = int.from_bytes(msg_length_bytes, "big", signed=False)
            msg = bytearray()
            #print(msg_length)
            while len(msg) < msg_length:
                remaining_bytes =  msg_length - len(msg)
                resp = win32file.ReadFile(handle, min(buffer_size, remaining_bytes))[1]
                msg.extend(resp)
            return msg
    except pywintypes.error as e:
        if e.args[0] == 2:
            raise Exception("no pipe")
        elif e.args[0] == 109:
            raise Exception("broken pipe, bye bye")


def write_msg(handle, data):
    try:
        msg_length_bytes = struct.pack(">I", len(data))
        win32file.WriteFile(handle, msg_length_bytes)
        win32file.WriteFile(handle, data)
    except pywintypes.error as e:
        if e.args[0] == 2:
            raise Exception("no pipe")
        elif e.args[0] == 109:
            raise Exception("broken pipe, bye bye")


class ImageProcessServerConnect:

    def __init__(self, cache_dir: str, threaded_reads: bool, working_dir = "./"):
        subprocess.Popen(["img_process_server.exe"])
        self.handle = connect_to_pipe(SERVER_PIPE_NAME)
        self.send_command("setup", [cache_dir, working_dir, str(threaded_reads).lower()])
        self.current_command = None

    def send_command(self, command_type : str, args : list):
        command_string = command_type + "|" + "|".join([str(arg) for arg in args])
        self.current_command = command_string
        write_msg(self.handle, command_string.encode(encoding="utf-8"))


    def _ask_for_images(self, paths : list, width : int, height : int, output_images: list):
        self.send_command("gets", [width, height] + paths)
        for _ in paths:
            data = read_msg(self.handle, READ_BUFFER_SIZE)
            image = cv2.imdecode(np.frombuffer(data, dtype=np.uint8), cv2.IMREAD_COLOR)
            output_images.append(image)
        self.current_command = None

    def ask_for_images(self, paths : list, width : int, height : int) -> list:
        while self.current_command != None:
            time.sleep(0.1)
        output_images = []
        for _ in range(0, MAX_RETRY):
            thread = Thread(target=self._ask_for_images, args=(paths, width, height, output_images))
            thread.start()
            thread.join(timeout=READ_TIMEOUT)
            if thread.is_alive():
                print("Image request timeouted. Trying again")
                win32file.CloseHandle(self.handle)
                time.sleep(0.5)
                self.handle = connect_to_pipe(SERVER_PIPE_NAME)
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
    benchmark("<Some path here>")