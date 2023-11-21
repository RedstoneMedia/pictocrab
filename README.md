# PictoCrab
PictoCrab is an image processing and caching server that uses pipes to load and resize images with speed🚀 and reliability 💯🙌. It is written in Rust 🦀, a fast 💨🚀, reliable 🔒👍, and memory-efficient 💾🌟👏 programming language that can handle concurrent and parallel tasks.

## Features 🚀
- PictoCrab can process images through pipes, which are fast, secure, and easy to use ⚡
- PictoCrab can cache images in memory or on disk (if not enough RAM is available), which can:
    - improve the performance and efficiency of the server 🚀
    - reduce the network traffic and bandwidth consumption 🌐
- PictoCrab allows requesting multiple images at once (to leverage multi-threading), which can increase the throughput and scalability of the server 🚀
- PictoCrab can load images from disk 💾 with a specific resolution or from a HTTP server 🌈

## Requirements
PictoCrab requires Windows to run, as it uses Windows named pipes to communicate.

## Usage
To use PictoCrab, you need to send commands to the server through pipes. \
For an example please look at:
[img_process_server_connect.py](img_process_server_connect.py)
