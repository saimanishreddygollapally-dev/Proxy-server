# 🌐 Multithreaded HTTP Proxy Server

A high-performance multithreaded HTTP proxy server written in **C** using **POSIX sockets** and **pthreads**. The proxy accepts client requests, forwards HTTP GET requests to destination servers, caches frequently accessed responses using an **LRU (Least Recently Used)** cache, and serves cached content to reduce latency and network traffic.

---

## 🚀 Features

- Multithreaded client handling using POSIX threads
- HTTP GET request forwarding
- LRU cache for faster repeated requests
- Thread-safe cache access
- Concurrent client support
- HTTP request parsing
- Cache hit and cache miss handling
- Socket programming using Berkeley sockets
- Modular project structure

---

## 🏗️ Architecture

```
                 Client
                    │
                    ▼
          Proxy Server (C)
                    │
        ┌───────────┴───────────┐
        │                       │
   Cache Hit               Cache Miss
        │                       │
        ▼                       ▼
 Return Cached Data      Remote Web Server
        │                       │
        └───────────┬───────────┘
                    ▼
            Store in LRU Cache
                    │
                    ▼
               Send Response
```

---

## 🛠️ Tech Stack

- C
- POSIX Threads (pthread)
- Berkeley Socket API
- HTTP Protocol
- LRU Cache
- Linux System Programming

---

## 📂 Project Structure

```text
Proxy-Server/
│
├── proxy_server.c
├── proxy_parse.c
├── proxy_parse.h
├── cache.c
├── cache.h
├── Makefile
├── README.md
└── images/
```

*(Adjust the filenames if your project uses different names.)*

---

## ⚙️ How It Works

1. Client sends an HTTP GET request to the proxy server.
2. The proxy parses the incoming request.
3. The cache is searched for the requested resource.
4. If the resource exists:
   - Return the cached response.
5. Otherwise:
   - Connect to the destination web server.
   - Forward the request.
   - Receive the response.
   - Store the response in the LRU cache.
   - Send the response back to the client.

---

## 🔄 Cache Management

The proxy implements an **LRU (Least Recently Used)** cache.

- Frequently accessed pages remain in cache.
- Least recently used entries are evicted when the cache reaches its capacity.
- Cache lookups reduce response latency and network requests.

---

## 🧵 Concurrency

Each incoming client connection is handled by a separate thread.

This allows the proxy to:

- Serve multiple clients simultaneously.
- Process independent requests concurrently.
- Protect shared cache data using synchronization primitives.

---

## 📦 Installation

Clone the repository

```bash
git clone https://github.com/your-username/Proxy-Server.git
```

Navigate to the project

```bash
cd Proxy-Server
```

Compile

```bash
make
```

Run

```bash
./proxy_server <port_number>
```

Example

```bash
./proxy_server 8080
```

---

## 🎯 Skills Demonstrated

- Socket Programming
- Computer Network
- Operating Systems
- POSIX Threads
- Concurrent Programming
- Synchronization
- HTTP Protocol
- Cache Design
- System Programming
- Memory Management
- Linux Development

---