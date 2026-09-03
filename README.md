# 🌐 Multithreaded HTTP Proxy Server

A multithreaded HTTP proxy server written in **C** using **POSIX sockets** and **pthreads**. The proxy accepts client connections, processes requests through an **8-thread worker pool**, forwards HTTP requests to remote servers, and caches GET responses using a **thread-safe LRU cache**.

---

## 🚀 Features

- Multithreaded client request processing using an **8-thread worker pool**
- Bounded **producer-consumer queue** for managing client connections
- HTTP **GET, POST, PUT, and DELETE** request handling
- HTTP request parsing and forwarding
- Request-body forwarding for POST and PUT requests
- Thread-safe **LRU cache** for GET responses
- **200 MB** maximum cache capacity
- **10 MB** maximum cache entry size
- Cache hit and cache miss tracking
- Thread-safe runtime metrics
- Request latency and active request tracking
- Concurrent client support
- Stress testing with **500 concurrent requests**

---

## 🏗️ Architecture

```text
                         Client
                           │
                           ▼
                  ┌─────────────────┐
                  │   Proxy Server  │
                  │      (C)        │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  Bounded Queue  │
                  │ Producer/Consumer│
                  └────────┬────────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
          Worker 1      Worker 2   ...  Worker 8
             │             │             │
             └─────────────┼─────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │    GET Cache    │
                  │ Thread-Safe LRU │
                  └────────┬────────┘
                           │
                    ┌──────┴──────┐
                    │             │
                 Cache Hit     Cache Miss
                    │             │
                    │             ▼
                    │    ┌─────────────────┐
                    │    │ Remote Server   │
                    │    └────────┬────────┘
                    │             │
                    │             ▼
                    │      Store Response
                    │       in LRU Cache
                    │             │
                    └──────┬──────┘
                           ▼
                    Send Response
                      to Client
git clone https://github.com/saimanishreddygollapally-dev/Proxy-server.git
Navigate to the Project
cd Proxy-server
Compile
gcc proxy_server.c proxy_parse.c -o proxy_server -lpthread
Run
./proxy_server

The proxy listens on:

localhost:8080
🧠 Key Concepts Demonstrated
Socket Programming
TCP/IP Networking
HTTP Protocol
Client-Server Architecture
Multithreading
POSIX Threads
Thread Pools
Producer-Consumer Pattern
Bounded Queues
Mutex Synchronization
Condition Variables
Thread-Safe Data Structures
LRU Cache Design
Memory Management
Runtime Metrics
Concurrent Load Testing
🎯 Project Highlights
8-thread fixed worker pool
1024-entry bounded connection queue
200 MB total cache capacity
10 MB maximum cache entry
HTTP GET, POST, PUT, DELETE support
Thread-safe LRU caching
Thread-safe runtime metrics
500 concurrent request stress test
0 proxy errors during the stress test
📌 Future Improvements

Possible extensions include:

Efficient O(1) cache lookup using a hash table
Doubly linked list based LRU implementation
Improved HTTP request-body framing
Robust handling of partial socket sends
HTTP persistent connection support
More detailed latency percentiles
Cache eviction statistics
HTTPS proxy support