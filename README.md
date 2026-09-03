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
```

---

## 🛠️ Tech Stack

- **C**
- **POSIX Sockets**
- **pthreads**
- **TCP/IP**
- **HTTP Protocol**
- **Mutexes**
- **Condition Variables**
- **Producer-Consumer Pattern**
- **LRU Cache**
- **Linux System Programming**

---

## 📂 Project Structure

```text
Proxy-Server/
│
├── proxy_server.c
├── proxy_parse.c
├── proxy_parse.h
└── README.md
```

---

## ⚙️ How It Works

### 1. Client Connection

The proxy listens for incoming TCP connections on port `8080`.

### 2. Request Queuing

Incoming client sockets are placed into a bounded producer-consumer queue.

The queue prevents unlimited pending connections and provides controlled work distribution among the worker threads.

### 3. Worker Pool

The proxy maintains **8 worker threads**.

Each worker retrieves a client socket from the queue and processes the request.

### 4. Request Parsing

The worker parses the HTTP request and determines the requested HTTP method and destination server.

### 5. Cache Lookup

For GET requests, the proxy checks the thread-safe LRU cache.

```text
                GET Request
                     │
                     ▼
                Cache Lookup
                 /        \
                /          \
             Hit            Miss
              │               │
              ▼               ▼
        Return Cached     Connect to
           Response       Remote Server
                              │
                              ▼
                         Get Response
                              │
                              ▼
                        Store in Cache
                              │
                              ▼
                       Send to Client
```

POST, PUT, and DELETE requests bypass the cache and are forwarded directly to the remote server.

### 6. Remote Communication

On a cache miss, the proxy establishes a TCP connection with the remote server, forwards the HTTP request, receives the response, and sends the response back to the client.

---

## 📡 HTTP Request Handling

The proxy supports the following HTTP methods:

| Method | Behavior |
|--------|----------|
| **GET** | Cache lookup followed by remote request on cache miss |
| **POST** | Forward request and body to remote server |
| **PUT** | Forward request and body to remote server |
| **DELETE** | Forward request to remote server |

GET responses are cached because GET is naturally suitable for response caching.

POST, PUT, and DELETE bypass the cache because they may modify server-side state.

---

## 🔄 LRU Cache

The proxy implements an **LRU (Least Recently Used)** cache for GET responses.

Each cache entry stores:

- Requested URL
- Response data
- Response length
- Last access timestamp
- Pointer to the next cache entry

When a cached URL is accessed, its access timestamp is updated.

When the cache reaches its capacity, the entry with the oldest access timestamp is removed.

### Cache Limits

```text
Maximum Cache Capacity: 200 MB
Maximum Cache Entry:     10 MB
```

The cache is protected using a **pthread mutex** to ensure safe concurrent access by multiple worker threads.

---

## 🧵 Concurrency Model

The proxy uses a fixed **8-thread worker pool** rather than creating a new thread for every incoming connection.

A bounded queue is used between the main thread and worker threads.

```text
Main Thread
     │
     │ Accept Client
     ▼
┌───────────────┐
│ Bounded Queue │
└───────┬───────┘
        │
        ├──────────► Worker 1
        ├──────────► Worker 2
        ├──────────► Worker 3
        ├──────────► ...
        └──────────► Worker 8
```

Synchronization is provided using:

- Mutexes
- Condition variables
- Shared queue protection
- Thread-safe cache operations
- Thread-safe metrics

The bounded queue also provides backpressure when all workers are busy.

---

## 📊 Runtime Metrics

The proxy includes thread-safe runtime metrics for monitoring request processing.

The following statistics are tracked:

- Total requests
- GET requests
- POST requests
- PUT requests
- DELETE requests
- Cache hits
- Cache misses
- Cache hit rate
- Request errors
- Active requests
- Average processing latency

Metrics are updated safely across worker threads using synchronization primitives.

---

## 🧪 Testing

The proxy was tested using `curl` and concurrent client requests.

### GET

```bash
curl -v -x http://localhost:8080 http://httpbin.org/get
```

### POST

```bash
curl -v -x http://localhost:8080 \
     -H "Content-Type: application/json" \
     -d '{"name":"Manish","project":"Proxy"}' \
     http://httpbin.org/post
```

### PUT

```bash
curl -v -x http://localhost:8080 \
     -X PUT \
     -H "Content-Type: text/plain" \
     -d "Hello from PUT" \
     http://httpbin.org/put
```

### DELETE

```bash
curl -v -x http://localhost:8080 \
     -X DELETE \
     http://httpbin.org/delete
```

### Large Request Bodies

Large POST and PUT request bodies were also tested to verify request-body forwarding across multiple socket reads.

---

## ⚡ Concurrent Stress Test

The proxy was stress-tested with **500 concurrent client requests** using the 8-thread worker pool.

Example result:

```text
========== Proxy Metrics ==========

Total Requests: 500
GET Requests: 500
POST Requests: 0
PUT Requests: 0
DELETE Requests: 0

Cache Hits: 492
Cache Misses: 8
Cache Hit Rate: 98.40%

Errors: 0
Active Requests: 0
Average Latency: 12.16 ms

===================================
```

The test used repeated GET requests to the same resource, making it a highly cache-friendly workload. Therefore, the reported cache hit rate and latency are specific to this test workload rather than general benchmark claims.

---

## 📦 Installation

### Clone the Repository

```bash
git clone https://github.com/saimanishreddygollapally-dev/Proxy-server.git
```

### Navigate to the Project

```bash
cd Proxy-server
```

### Compile

```bash
gcc proxy_server.c proxy_parse.c -o proxy_server -lpthread
```

### Run

```bash
./proxy_server
```

The proxy listens on:

```text
localhost:8080
```

---

## 🧠 Key Concepts Demonstrated

- Socket Programming
- TCP/IP Networking
- HTTP Protocol
- Client-Server Architecture
- Multithreading
- POSIX Threads
- Thread Pools
- Producer-Consumer Pattern
- Bounded Queues
- Mutex Synchronization
- Condition Variables
- Thread-Safe Data Structures
- LRU Cache Design
- Memory Management
- Runtime Metrics
- Concurrent Load Testing

---

## 🎯 Project Highlights

- **8-thread** fixed worker pool
- **1024-entry** bounded connection queue
- **200 MB** total cache capacity
- **10 MB** maximum cache entry
- HTTP **GET, POST, PUT, DELETE** support
- Thread-safe LRU caching
- Thread-safe runtime metrics
- **500 concurrent request** stress test
- **0 proxy errors** during the stress test

---

## 📌 Future Improvements

Possible extensions include:

- Efficient O(1) cache lookup using a hash table
- Doubly linked list based LRU implementation
- Improved HTTP request-body framing
- Robust handling of partial socket sends
- HTTP persistent connection support
- More detailed latency percentiles
- Cache eviction statistics
- HTTPS proxy support
