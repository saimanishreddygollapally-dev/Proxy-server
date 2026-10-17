#include "proxy_parse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>
#include <signal.h>

#include <stdint.h>

#define MAX_BYTES 4096    //max allowed size of request/response
#define MAX_SIZE 200*(1<<20)     //size of the cache
#define MAX_ELEMENT_SIZE 10*(1<<20)     //max size of an element in cache
#define MAX_THREADS 8
#define QUEUE_SIZE 1024
typedef struct cache_element cache_element;

struct cache_element{
    char* data;         //data stores response
    int len;          //length of data i.e.. sizeof(data)...
    char* url;        //url stores the request
	time_t lru_time_track;    //lru_time_track stores the latest time the element is  accesed
    cache_element* next;    //pointer to next element
};

cache_element* find(char* url);
int add_cache_element(char* data,int size,char* url);
void remove_cache_element();

int port_number = 8080;				// Default Port
int proxy_socketId;					// socket descriptor of proxy server         //array to store the thread ids of clients                //if client requests exceeds the max_clients this seamaphore puts the
                                    //waiting threads to sleep and wakes them when traffic on queue decreases
//sem_t cache_lock;			       
pthread_mutex_t lock;               //lock is used for locking the cache
pthread_mutex_t metrics_lock;       //lock is used for locking the metrics counters

long total_requests = 0;
long get_requests = 0;
long post_requests = 0;
long put_requests = 0;
long delete_requests = 0;
long cache_hits = 0;
long cache_misses = 0;
long errors = 0;
long active_requests = 0;
long completed_requests = 0;
double total_processing_ms = 0.0;

void metrics_inc(long *counter)
{
	pthread_mutex_lock(&metrics_lock);
	(*counter)++;
	pthread_mutex_unlock(&metrics_lock);
}

void print_metrics()
{
	long t, g, p, u, d, hits, misses, err, active, completed;
	double total_ms, hit_rate, avg;

	pthread_mutex_lock(&metrics_lock);
	t = total_requests;
	g = get_requests;
	p = post_requests;
	u = put_requests;
	d = delete_requests;
	hits = cache_hits;
	misses = cache_misses;
	err = errors;
	active = active_requests;
	completed = completed_requests;
	total_ms = total_processing_ms;
	pthread_mutex_unlock(&metrics_lock);

	hit_rate = 0.0;
	if(hits + misses > 0)
		hit_rate = (100.0 * hits) / (hits + misses);

	avg = 0.0;
	if(completed > 0)
		avg = total_ms / completed;

	printf("========== Proxy Metrics ==========\n");
	printf("Total Requests: %ld\n", t);
	printf("GET Requests: %ld\n", g);
	printf("POST Requests: %ld\n", p);
	printf("PUT Requests: %ld\n", u);
	printf("DELETE Requests: %ld\n", d);
	printf("Cache Hits: %ld\n", hits);
	printf("Cache Misses: %ld\n", misses);
	printf("Cache Hit Rate: %.2f%%\n", hit_rate);
	printf("Errors: %ld\n", err);
	printf("Active Requests: %ld\n", active);
	printf("Average Latency: %.2f ms\n", avg);
	printf("===================================\n");
	fflush(stdout);
}

void handle_sigint(int sig)
{
	(void)sig;
	print_metrics();
	exit(0);
}

cache_element* head;                //pointer to the cache
int cache_size;             //cache_size denotes the current size of the cache
int dequeue();
void enqueue(int socket);
void handle_client(int socket);

int handle_request(int clientSocket,
                   struct ParsedRequest *request,
                   char *tempReq);
int handle_post_request(int clientSocket,
                        struct ParsedRequest *request,
                        char *body,
                        int body_len);
int handle_put_request(int clientSocket,
                       struct ParsedRequest *request,
                       char *body,
                       int body_len);
int handle_delete_request(int clientSocket,
                          struct ParsedRequest *request,
                          char *body,
                          int body_len);
int sendErrorMessage(int socket, int status_code)
{
	char str[1024];
	char currentTime[50];
	time_t now = time(0);

	struct tm data = *gmtime(&now);
	strftime(currentTime,sizeof(currentTime),"%a, %d %b %Y %H:%M:%S %Z", &data);

	metrics_inc(&errors);

	switch(status_code)
	{
		case 400: snprintf(str, sizeof(str), "HTTP/1.1 400 Bad Request\r\nContent-Length: 95\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>400 Bad Request</TITLE></HEAD>\n<BODY><H1>400 Bad Rqeuest</H1>\n</BODY></HTML>", currentTime);
				  printf("400 Bad Request\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 403: snprintf(str, sizeof(str), "HTTP/1.1 403 Forbidden\r\nContent-Length: 112\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>403 Forbidden</TITLE></HEAD>\n<BODY><H1>403 Forbidden</H1><br>Permission Denied\n</BODY></HTML>", currentTime);
				  printf("403 Forbidden\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 404: snprintf(str, sizeof(str), "HTTP/1.1 404 Not Found\r\nContent-Length: 91\r\nContent-Type: text/html\r\nConnection: keep-alive\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Found</TITLE></HEAD>\n<BODY><H1>404 Not Found</H1>\n</BODY></HTML>", currentTime);
				  printf("404 Not Found\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 500: snprintf(str, sizeof(str), "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 115\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>500 Internal Server Error</TITLE></HEAD>\n<BODY><H1>500 Internal Server Error</H1>\n</BODY></HTML>", currentTime);
				  //printf("500 Internal Server Error\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 501: snprintf(str, sizeof(str), "HTTP/1.1 501 Not Implemented\r\nContent-Length: 103\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>404 Not Implemented</TITLE></HEAD>\n<BODY><H1>501 Not Implemented</H1>\n</BODY></HTML>", currentTime);
				  printf("501 Not Implemented\n");
				  send(socket, str, strlen(str), 0);
				  break;

		case 505: snprintf(str, sizeof(str), "HTTP/1.1 505 HTTP Version Not Supported\r\nContent-Length: 125\r\nConnection: keep-alive\r\nContent-Type: text/html\r\nDate: %s\r\nServer: VaibhavN/14785\r\n\r\n<HTML><HEAD><TITLE>505 HTTP Version Not Supported</TITLE></HEAD>\n<BODY><H1>505 HTTP Version Not Supported</H1>\n</BODY></HTML>", currentTime);
				  printf("505 HTTP Version Not Supported\n");
				  send(socket, str, strlen(str), 0);
				  break;

		default:  return -1;

	}
	return 1;
}

int connectRemoteServer(char* host_addr, int port_num)
{
	// Creating Socket for remote server ---------------------------

	int remoteSocket = socket(AF_INET, SOCK_STREAM, 0);

	if( remoteSocket < 0)
	{
		printf("Error in Creating Socket.\n");
		return -1;
	}
	
	// Get host by the name or ip address provided

	struct hostent *host = gethostbyname(host_addr);	
	if(host == NULL)
	{
		fprintf(stderr, "No such host exists.\n");	
		return -1;
	}

	// inserts ip address and port number of host in struct `server_addr`
	struct sockaddr_in server_addr;

	bzero((char*)&server_addr, sizeof(server_addr));
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port_num);

	if (host == NULL || host->h_addr_list == NULL ||
    host->h_addr_list[0] == NULL ||
    host->h_length != sizeof(server_addr.sin_addr)) {
    fprintf(stderr, "Hostname resolution failed or returned an invalid address\n");
    close(remoteSocket);
    return -1;
}

memcpy(&server_addr.sin_addr, host->h_addr_list[0],
       sizeof(server_addr.sin_addr));
	
	// Connect to Remote server ----------------------------------------------------

	if( connect(remoteSocket, (struct sockaddr*)&server_addr, (socklen_t)sizeof(server_addr)) < 0 )
	{
		perror("connect");
		return -1;
	}
	// free(host_addr);
	return remoteSocket;
}



int handle_request(int clientSocket, struct ParsedRequest *request, char *tempReq)
{
    size_t capacity = MAX_BYTES;
    char *buf = calloc(capacity, 1);

    if (buf == NULL) {
        perror("calloc buf");
        return -1;
    }

    int n = snprintf(buf, capacity, "GET %s %s\r\n",
                     request->path, request->version);

    if (n < 0 || (size_t)n >= capacity) {
        fprintf(stderr, "Request line too large\n");
        free(buf);
        return -1;
    }

    size_t len = (size_t)n;

    if (ParsedHeader_set(request, "Connection", "close") < 0) {
        fprintf(stderr, "Failed to set Connection header\n");
        free(buf);
        return -1;
    }

    if (ParsedHeader_get(request, "Host") == NULL) {
        if (ParsedHeader_set(request, "Host", request->host) < 0) {
            fprintf(stderr, "Failed to set Host header\n");
            free(buf);
            return -1;
        }
    }

    if (ParsedRequest_unparse_headers(request, buf + len,
                                      capacity - len) < 0) {
        fprintf(stderr, "Failed to serialize request headers\n");
        free(buf);
        return -1;
    }

    char *terminator = memchr(buf, '\0', capacity);
    if (terminator == NULL) {
        fprintf(stderr, "Request is not NUL-terminated\n");
        free(buf);
        return -1;
    }

    size_t request_len = (size_t)(terminator - buf);

    int server_port = 80;
    if (request->port != NULL)
        server_port = atoi(request->port);

    printf("Host: %s\n", request->host);
    printf("Port: %d\n", server_port);
    printf("Path: %s\n", request->path);

    int remoteSocketID = connectRemoteServer(request->host, server_port);

    if (remoteSocketID < 0) {
        free(buf);
        return -1;
    }

    size_t total_sent = 0;
    while (total_sent < request_len) {
        ssize_t sent = send(remoteSocketID, buf + total_sent,
                            request_len - total_sent, 0);

        if (sent <= 0) {
            perror("send upstream request");
            close(remoteSocketID);
            free(buf);
            return -1;
        }

        total_sent += (size_t)sent;
    }

    char *temp_buffer = malloc(capacity + 1);
    if (temp_buffer == NULL) {
        perror("malloc temp_buffer");
        close(remoteSocketID);
        free(buf);
        return -1;
    }

    size_t temp_buffer_size = capacity + 1;
    size_t temp_buffer_index = 0;
    ssize_t bytes_send;

    while ((bytes_send = recv(remoteSocketID, buf,
                              capacity, 0)) > 0) {
        size_t total_client_sent = 0;

        while (total_client_sent < (size_t)bytes_send) {
            ssize_t sent = send(
                clientSocket,
                buf + total_client_sent,
                (size_t)bytes_send - total_client_sent,
                0
            );

            if (sent <= 0) {
                perror("send response to client");
                free(temp_buffer);
                close(remoteSocketID);
                free(buf);
                return -1;
            }

            total_client_sent += (size_t)sent;
        }

        size_t required =
            temp_buffer_index + (size_t)bytes_send + 1;

        if (required > temp_buffer_size) {
            size_t new_size = temp_buffer_size;

            while (new_size < required) {
                if (new_size > SIZE_MAX / 2) {
                    fprintf(stderr, "Response too large to cache\n");
                    free(temp_buffer);
                    close(remoteSocketID);
                    free(buf);
                    return -1;
                }
                new_size *= 2;
            }

            char *new_buffer = realloc(temp_buffer, new_size);
            if (new_buffer == NULL) {
                perror("realloc temp_buffer");
                free(temp_buffer);
                close(remoteSocketID);
                free(buf);
                return -1;
            }

            temp_buffer = new_buffer;
            temp_buffer_size = new_size;
        }

        memcpy(temp_buffer + temp_buffer_index,
               buf, (size_t)bytes_send);

        temp_buffer_index += (size_t)bytes_send;
    }

    if (bytes_send < 0) {
        perror("recv from upstream server");
        free(temp_buffer);
        close(remoteSocketID);
        free(buf);
        return -1;
    }

    temp_buffer[temp_buffer_index] = '\0';

    close(remoteSocketID);
    free(buf);

    add_cache_element(temp_buffer, temp_buffer_index, tempReq);

    free(temp_buffer);
    printf("Done\n");

    return 0;
}

int handle_post_request(int clientSocket, struct ParsedRequest *request, char *body, int body_len)
{
	char *buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	strcpy(buf, "POST ");
	strcat(buf, request->path);
	strcat(buf, " ");
	strcat(buf, request->version);
	strcat(buf, "\r\n");

	size_t len = strlen(buf);

	if (ParsedHeader_set(request, "Connection", "close") < 0){
		printf("set header key not work\n");
	}

	if(ParsedHeader_get(request, "Host") == NULL)
	{
		if(ParsedHeader_set(request, "Host", request->host) < 0){
			printf("Set \"Host\" header key not working\n");
		}
	}

	if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
		printf("unparse failed\n");
	}

	int server_port = 80;
	if(request->port != NULL)
		server_port = atoi(request->port);
	printf("Host: %s\n", request->host);
	printf("Port: %d\n", server_port);
	printf("Path: %s\n", request->path);
	int remoteSocketID = connectRemoteServer(request->host, server_port);

	if(remoteSocketID < 0)
	{
		free(buf);
		return -1;
	}

	// send request headers to remote server
	int total_sent = 0;
	int header_len = strlen(buf);
	while(total_sent < header_len)
	{
		int sent = send(remoteSocketID, buf + total_sent, header_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			free(buf);
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}
	free(buf);

	// send POST body to remote server
	total_sent = 0;
	while(total_sent < body_len)
	{
		int sent = send(remoteSocketID, body + total_sent, body_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}

	buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	bzero(buf, MAX_BYTES);

	int bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);

	while(bytes_send > 0)
	{
		total_sent = 0;
		while(total_sent < bytes_send)
		{
			int sent = send(clientSocket, buf + total_sent, bytes_send - total_sent, 0);
			if(sent <= 0)
			{
				perror("send failed");
				break;
			}
			total_sent += sent;
		}

		if(bytes_send < 0)
		{
			perror("Error in sending data to client socket.\n");
			break;
		}
		bzero(buf, MAX_BYTES);
		bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);
	}
	free(buf);
	printf("Done\n");
	close(remoteSocketID);
	return 0;
}

int handle_put_request(int clientSocket, struct ParsedRequest *request, char *body, int body_len)
{
	char *buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	strcpy(buf, "PUT ");
	strcat(buf, request->path);
	strcat(buf, " ");
	strcat(buf, request->version);
	strcat(buf, "\r\n");

	size_t len = strlen(buf);

	if (ParsedHeader_set(request, "Connection", "close") < 0){
		printf("set header key not work\n");
	}

	if(ParsedHeader_get(request, "Host") == NULL)
	{
		if(ParsedHeader_set(request, "Host", request->host) < 0){
			printf("Set \"Host\" header key not working\n");
		}
	}

	if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
		printf("unparse failed\n");
	}

	int server_port = 80;
	if(request->port != NULL)
		server_port = atoi(request->port);
	printf("Host: %s\n", request->host);
	printf("Port: %d\n", server_port);
	printf("Path: %s\n", request->path);
	int remoteSocketID = connectRemoteServer(request->host, server_port);

	if(remoteSocketID < 0)
	{
		free(buf);
		return -1;
	}

	// send request headers to remote server
	int total_sent = 0;
	int header_len = strlen(buf);
	while(total_sent < header_len)
	{
		int sent = send(remoteSocketID, buf + total_sent, header_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			free(buf);
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}
	free(buf);

	// send PUT body to remote server
	total_sent = 0;
	while(total_sent < body_len)
	{
		int sent = send(remoteSocketID, body + total_sent, body_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}

	buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	bzero(buf, MAX_BYTES);

	int bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);

	while(bytes_send > 0)
	{
		total_sent = 0;
		while(total_sent < bytes_send)
		{
			int sent = send(clientSocket, buf + total_sent, bytes_send - total_sent, 0);
			if(sent <= 0)
			{
				perror("send failed");
				break;
			}
			total_sent += sent;
		}

		if(bytes_send < 0)
		{
			perror("Error in sending data to client socket.\n");
			break;
		}
		bzero(buf, MAX_BYTES);
		bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);
	}
	free(buf);
	printf("Done\n");
	close(remoteSocketID);
	return 0;
}

int handle_delete_request(int clientSocket, struct ParsedRequest *request, char *body, int body_len)
{
	char *buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	strcpy(buf, "DELETE ");
	strcat(buf, request->path);
	strcat(buf, " ");
	strcat(buf, request->version);
	strcat(buf, "\r\n");

	size_t len = strlen(buf);

	if (ParsedHeader_set(request, "Connection", "close") < 0){
		printf("set header key not work\n");
	}

	if(ParsedHeader_get(request, "Host") == NULL)
	{
		if(ParsedHeader_set(request, "Host", request->host) < 0){
			printf("Set \"Host\" header key not working\n");
		}
	}

	if (ParsedRequest_unparse_headers(request, buf + len, (size_t)MAX_BYTES - len) < 0) {
		printf("unparse failed\n");
	}

	int server_port = 80;
	if(request->port != NULL)
		server_port = atoi(request->port);
	printf("Host: %s\n", request->host);
	printf("Port: %d\n", server_port);
	printf("Path: %s\n", request->path);
	int remoteSocketID = connectRemoteServer(request->host, server_port);

	if(remoteSocketID < 0)
	{
		free(buf);
		return -1;
	}

	// send request headers to remote server
	int total_sent = 0;
	int header_len = strlen(buf);
	while(total_sent < header_len)
	{
		int sent = send(remoteSocketID, buf + total_sent, header_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			free(buf);
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}
	free(buf);

	// send DELETE body to remote server if present
	total_sent = 0;
	while(total_sent < body_len)
	{
		int sent = send(remoteSocketID, body + total_sent, body_len - total_sent, 0);
		if(sent <= 0)
		{
			perror("send failed");
			close(remoteSocketID);
			return -1;
		}
		total_sent += sent;
	}

	buf = (char*)malloc(sizeof(char)*MAX_BYTES);
	bzero(buf, MAX_BYTES);

	int bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);

	while(bytes_send > 0)
	{
		total_sent = 0;
		while(total_sent < bytes_send)
		{
			int sent = send(clientSocket, buf + total_sent, bytes_send - total_sent, 0);
			if(sent <= 0)
			{
				perror("send failed");
				break;
			}
			total_sent += sent;
		}

		if(bytes_send < 0)
		{
			perror("Error in sending data to client socket.\n");
			break;
		}
		bzero(buf, MAX_BYTES);
		bytes_send = recv(remoteSocketID, buf, MAX_BYTES-1, 0);
	}
	free(buf);
	printf("Done\n");
	close(remoteSocketID);
	return 0;
}

int checkHTTPversion(char *msg)
{
	int version = -1;

	if(strncmp(msg, "HTTP/1.1", 8) == 0)
	{
		version = 1;
	}
	else if(strncmp(msg, "HTTP/1.0", 8) == 0)			
	{
		version = 1;										// Handling this similar to version 1.1
	}
	else
		version = -1;

	return version;
}

void* worker_thread(void* arg)
{	(void)arg;
    while(1)
    {
        int socket = dequeue();

        handle_client(socket);
    }

    return NULL;
}
void handle_client(int socket)
{     // Socket is socket descriptor of the connected Client
	int bytes_send_client,len;	  // Bytes Transferred
	struct timespec start_time, end_time;

	clock_gettime(CLOCK_MONOTONIC, &start_time);
	pthread_mutex_lock(&metrics_lock);
	active_requests++;
	pthread_mutex_unlock(&metrics_lock);

	
	char *buffer = (char*)calloc(MAX_BYTES,sizeof(char));	// Creating buffer of 4kb for a client
	
	
	bzero(buffer, MAX_BYTES);								// Making buffer zero
	bytes_send_client = recv(socket, buffer, MAX_BYTES, 0); // Receiving the Request of client by proxy server
	
	while(bytes_send_client > 0)
	{
		len = strlen(buffer);
        //loop until u find "\r\n\r\n" in the buffer
		if(strstr(buffer, "\r\n\r\n") == NULL)
		{	
			bytes_send_client = recv(socket, buffer + len, MAX_BYTES - len, 0);
		}
		else{
			break;
		}
	}

	// printf("--------------------------------------------\n");
	// printf("%s\n",buffer);
	// printf("----------------------%d----------------------\n",strlen(buffer));
	
	char *tempReq = (char*)malloc(strlen(buffer)*sizeof(char)+1);
    //tempReq, buffer both store the http request sent by client
	strcpy(tempReq, buffer);

	if(bytes_send_client > 0)
	{
		metrics_inc(&total_requests);
		if(strncmp(buffer, "GET ", 4) == 0)
			metrics_inc(&get_requests);
		else if(strncmp(buffer, "POST ", 5) == 0)
			metrics_inc(&post_requests);
		else if(strncmp(buffer, "PUT ", 4) == 0)
			metrics_inc(&put_requests);
		else if(strncmp(buffer, "DELETE ", 7) == 0)
			metrics_inc(&delete_requests);
	}
	
	
    // Check the cache for GET requests.
    char *cached_response = NULL;
    size_t cached_response_len = 0;
    int cache_hit = 0;

    if (strncmp(buffer, "GET ", 4) == 0) {
        pthread_mutex_lock(&lock);

        cache_element *entry = head;

        while (entry != NULL) {
            if (strcmp(entry->url, tempReq) == 0) {
                entry->lru_time_track = time(NULL);

                cached_response_len = (size_t)entry->len;
                cached_response = malloc(cached_response_len);

                if (cached_response != NULL) {
                    memcpy(cached_response, entry->data,
                           cached_response_len);
                    cache_hit = 1;
                }

                break;
            }

            entry = entry->next;
        }

        pthread_mutex_unlock(&lock);

        if (cache_hit) {
            metrics_inc(&cache_hits);
        } else {
            metrics_inc(&cache_misses);
        }
    }

    if (cache_hit) {
        size_t pos = 0;

        while (pos < cached_response_len) {
            ssize_t sent = send(
                socket,
                cached_response + pos,
                cached_response_len - pos,
                0
            );

            if (sent <= 0) {
                perror("send cached response");
                metrics_inc(&errors);
                break;
            }

            pos += (size_t)sent;
        }

        free(cached_response);
    }

	
	
	else if(bytes_send_client > 0)
	{
		len = strlen(buffer); 
		//Parsing the request
		struct ParsedRequest *request = ParsedRequest_create();
		
        //ParsedRequest_parse returns 0 on success and -1 on failure.On success it stores parsed request in
        // the request
		if (ParsedRequest_parse(request, buffer, len) < 0) 
		{
		   	printf("Parsing failed\n");
			metrics_inc(&errors);
		}
		else
		{	
			if(!strcmp(request->method,"GET"))							
			{
				bzero(buffer, MAX_BYTES);
                
				if( request->host && request->path && (checkHTTPversion(request->version) == 1) )
				{
					bytes_send_client = handle_request(socket, request, tempReq);		// Handle GET request
					if(bytes_send_client == -1)
					{	
						sendErrorMessage(socket, 500);
					}

				}
				else
					sendErrorMessage(socket, 500);			// 500 Internal Error

			}
			else if(!strcmp(request->method,"POST"))
			{
				if( request->host && request->path && (checkHTTPversion(request->version) == 1) )
				{
					struct ParsedHeader *cl_header = ParsedHeader_get(request, "Content-Length");
					if(cl_header == NULL)
					{
						sendErrorMessage(socket, 400);
					}
					else
					{
						char *header_end = strstr(buffer, "\r\n\r\n");
						int header_len = (header_end - buffer) + 4;
						int content_length = atoi(cl_header->value);
						char *body = (char*)malloc(content_length + 1);
						int body_len = 0;
						int body_in_buffer = bytes_send_client - header_len;

						// copy body bytes already received with the first recv()
						if(body_in_buffer > 0)
						{
							memcpy(body, buffer + header_len, body_in_buffer);
							body_len = body_in_buffer;
						}

						// recv remaining body bytes from client
						while(body_len < content_length)
						{
							int n = recv(socket, body + body_len, content_length - body_len, 0);
							if(n <= 0)
								break;
							body_len += n;
						}

						if(body_len < content_length)
						{
							free(body);
							sendErrorMessage(socket, 400);
						}
						else
						{
							body[content_length] = '\0';
							bytes_send_client = handle_post_request(socket, request, body, body_len);
							free(body);
							if(bytes_send_client == -1)
								sendErrorMessage(socket, 500);
						}
					}
				}
				else
					sendErrorMessage(socket, 500);
			}
			else if(!strcmp(request->method,"PUT"))
			{
				if( request->host && request->path && (checkHTTPversion(request->version) == 1) )
				{
					struct ParsedHeader *cl_header = ParsedHeader_get(request, "Content-Length");
					if(cl_header == NULL)
					{
						sendErrorMessage(socket, 400);
					}
					else
					{
						char *header_end = strstr(buffer, "\r\n\r\n");
						int header_len = (header_end - buffer) + 4;
						int content_length = atoi(cl_header->value);
						char *body = (char*)malloc(content_length + 1);
						int body_len = 0;
						int body_in_buffer = bytes_send_client - header_len;

						// copy body bytes already received with the first recv()
						if(body_in_buffer > 0)
						{
							memcpy(body, buffer + header_len, body_in_buffer);
							body_len = body_in_buffer;
						}

						// recv remaining body bytes from client
						while(body_len < content_length)
						{
							int n = recv(socket, body + body_len, content_length - body_len, 0);
							if(n <= 0)
								break;
							body_len += n;
						}

						if(body_len < content_length)
						{
							free(body);
							sendErrorMessage(socket, 400);
						}
						else
						{
							body[content_length] = '\0';
							bytes_send_client = handle_put_request(socket, request, body, body_len);
							free(body);
							if(bytes_send_client == -1)
								sendErrorMessage(socket, 500);
						}
					}
				}
				else
					sendErrorMessage(socket, 500);
			}
			else if(!strcmp(request->method,"DELETE"))
			{
				if( request->host && request->path && (checkHTTPversion(request->version) == 1) )
				{
					struct ParsedHeader *cl_header = ParsedHeader_get(request, "Content-Length");
					char *body = NULL;
					int body_len = 0;
					int body_ok = 1;

					if(cl_header != NULL)
					{
						char *header_end = strstr(buffer, "\r\n\r\n");
						int header_len = (header_end - buffer) + 4;
						int content_length = atoi(cl_header->value);
						body = (char*)malloc(content_length + 1);
						body_len = 0;
						int body_in_buffer = bytes_send_client - header_len;

						// copy body bytes already received with the first recv()
						if(body_in_buffer > 0)
						{
							memcpy(body, buffer + header_len, body_in_buffer);
							body_len = body_in_buffer;
						}

						// recv remaining body bytes from client
						while(body_len < content_length)
						{
							int n = recv(socket, body + body_len, content_length - body_len, 0);
							if(n <= 0)
								break;
							body_len += n;
						}

						if(body_len < content_length)
						{
							free(body);
							body = NULL;
							body_ok = 0;
							sendErrorMessage(socket, 400);
						}
						else
						{
							body[content_length] = '\0';
						}
					}

					if(body_ok)
					{
						bytes_send_client = handle_delete_request(socket, request, body, body_len);
						if(body != NULL)
							free(body);
						if(bytes_send_client == -1)
							sendErrorMessage(socket, 500);
					}
				}
				else
					sendErrorMessage(socket, 500);
			}
            else
            {
                sendErrorMessage(socket, 501);
            }
    
		}
        //freeing up the request pointer
		ParsedRequest_destroy(request);

	}

	else if( bytes_send_client < 0)
	{
		perror("Error in receiving from client.\n");
		metrics_inc(&errors);
	}
	else if(bytes_send_client == 0)
	{
		printf("Client disconnected!\n");
	}

	shutdown(socket, SHUT_RDWR);
	close(socket);
	free(buffer);
	free(tempReq);

	clock_gettime(CLOCK_MONOTONIC, &end_time);
	double elapsed_ms = (end_time.tv_sec - start_time.tv_sec) * 1000.0
		+ (end_time.tv_nsec - start_time.tv_nsec) / 1000000.0;

	pthread_mutex_lock(&metrics_lock);
	active_requests--;
	completed_requests++;
	total_processing_ms += elapsed_ms;
	pthread_mutex_unlock(&metrics_lock);
}


typedef struct
{
    int sockets[QUEUE_SIZE];

    int front;
    int rear;
    int count;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;

} TaskQueue;

TaskQueue queue;

void enqueue(int socket)
{
    pthread_mutex_lock(&queue.mutex);
    while(queue.count == QUEUE_SIZE) pthread_cond_wait(&queue.not_full, &queue.mutex);
    queue.sockets[queue.rear] = socket;
    queue.rear = (queue.rear + 1) % QUEUE_SIZE;
    queue.count++;
    pthread_cond_signal(&queue.not_empty);
    pthread_mutex_unlock(&queue.mutex);
}
int dequeue(){

    pthread_mutex_lock(&queue.mutex);
    while(queue.count == 0) pthread_cond_wait(&queue.not_empty, &queue.mutex);
    int socket = queue.sockets[queue.front];
    queue.front = (queue.front + 1) % QUEUE_SIZE;
    queue.count--;
    pthread_cond_signal(&queue.not_full);
    pthread_mutex_unlock(&queue.mutex);
    return socket;
}

int main(int argc, char * argv[]) {

	int client_socketId, client_len; // client_socketId == to store the client socket id
	struct sockaddr_in server_addr, client_addr; // Address of client and server to be assigned
 // Initializing seamaphore and lock
    pthread_mutex_init(&lock,NULL); // Initializing lock for cache
    pthread_mutex_init(&metrics_lock,NULL);
    signal(SIGINT, handle_sigint);
    signal(SIGTERM, handle_sigint);
    

	if(argc == 2)        //checking whether two arguments are received or not
	{
		port_number = atoi(argv[1]);
	}
	else
	{
		printf("Too few arguments\n");
		exit(1);
	}

	printf("Setting Proxy Server Port : %d\n",port_number);

    //creating the proxy socket
	proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);

	if( proxy_socketId < 0)
	{
		perror("Failed to create socket.\n");
		exit(1);
	}

	int reuse =1;
	if (setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, (const char*)&reuse, sizeof(reuse)) < 0) 
        perror("setsockopt(SO_REUSEADDR) failed\n");

	bzero((char*)&server_addr, sizeof(server_addr));  
	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port_number); // Assigning port to the Proxy
	server_addr.sin_addr.s_addr = INADDR_ANY; // Any available adress assigned

    // Binding the socket
	if( bind(proxy_socketId, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0 )
	{
		perror("Port is not free\n");
		exit(1);
	}
	printf("Binding on port: %d\n",port_number);

    // Proxy socket listening to the requests
	int listen_status = listen(proxy_socketId, QUEUE_SIZE);

	if(listen_status < 0 )
	{
		perror("Error while Listening !\n");
		exit(1);
	}
  // This array stores socket descriptors of connected clients
	pthread_t workers[MAX_THREADS];
	queue.front = 0;
	queue.rear = 0;
	queue.count = 0;
	pthread_mutex_init(&queue.mutex,NULL);
	pthread_cond_init(&queue.not_empty,NULL);
	pthread_cond_init(&queue.not_full,NULL);
	for (int i = 0; i < MAX_THREADS; i++) pthread_create(&workers[i], NULL, worker_thread, NULL);
    
	while(1)	// Infinite Loop for accepting connections
	{
		
		bzero((char*)&client_addr, sizeof(client_addr));			// Clears struct client_addr
		client_len = sizeof(client_addr); 

        // Accepting the connections
		client_socketId = accept(proxy_socketId, (struct sockaddr*)&client_addr,(socklen_t*)&client_len);	// Accepts connection
		if(client_socketId < 0)
		{
			fprintf(stderr, "Error in Accepting connection !\n");
			exit(1);
		}
		enqueue(client_socketId);

		// Getting IP address and port number of client
		struct sockaddr_in* client_pt = (struct sockaddr_in*)&client_addr;
		struct in_addr ip_addr = client_pt->sin_addr;
		char str[INET_ADDRSTRLEN];										// INET_ADDRSTRLEN: Default ip address size
		inet_ntop( AF_INET, &ip_addr, str, INET_ADDRSTRLEN );
		printf("Client is connected with port number: %d and ip address: %s \n",ntohs(client_addr.sin_port), str);
		//printf("Socket values of index %d in main function is %d\n",i, client_socketId);
	}
	close(proxy_socketId);									// Close socket
 	return 0;
}

cache_element* find(char* url){

// Checks for url in the cache if found returns pointer to the respective cache element or else returns NULL
    cache_element* site=NULL;
	//sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 
    if(head!=NULL){
        site = head;
        while (site!=NULL)
        {
            if(!strcmp(site->url,url)){
				printf("LRU Time Track Before : %ld", site->lru_time_track);
                printf("\nurl found\n");
				// Updating the time_track
				site->lru_time_track = time(NULL);
				printf("LRU Time Track After : %ld", site->lru_time_track);
				break;
            }
            site=site->next;
        }       
    }
	else {
    printf("\nurl not found\n");
	}
	//sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
    return site;
}

void remove_cache_element(){
    // If cache is not empty searches for the node which has the least lru_time_track and deletes it
    cache_element * p ;  	// Cache_element Pointer (Prev. Pointer)
	cache_element * q ;		// Cache_element Pointer (Next Pointer)
	cache_element * temp;	// Cache element to remove
    //sem_wait(&cache_lock);
	if( head != NULL) { // Cache != empty
		for (q = head, p = head, temp =head ; q -> next != NULL; 
			q = q -> next) { // Iterate through entire cache and search for oldest time track
			if(( (q -> next) -> lru_time_track) < (temp -> lru_time_track)) {
				temp = q -> next;
				p = q;
			}
		}
		if(temp == head) { 
			head = head -> next; /*Handle the base case*/
		} else {
			p->next = temp->next;	
		}
		cache_size = cache_size - (temp -> len) - sizeof(cache_element) - 
		strlen(temp -> url) - 1;     //updating the cache size
		free(temp->data);     		
		free(temp->url); // Free the removed element 
		free(temp);
	} 
	//sem_post(&cache_lock);
}

int add_cache_element(char* data,int size,char* url){
    // Adds element to the cache
	// sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&lock);
	printf("Add Cache Lock Acquired %d\n", temp_lock_val);
    int element_size=size+1+strlen(url)+sizeof(cache_element); // Size of the new element which will be added to the cache
    if(element_size>MAX_ELEMENT_SIZE){
		//sem_post(&cache_lock);
        // If element size is greater than MAX_ELEMENT_SIZE we don't add the element to the cache
        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		// free(data);
		// printf("--\n");
		// free(url);
        return 0;
    }
    else
    {   while(cache_size+element_size>MAX_SIZE){
            // We keep removing elements from cache until we get enough space to add the element
            remove_cache_element();
        }
        cache_element* element = (cache_element*) malloc(sizeof(cache_element)); // Allocating memory for the new cache element
        element->data= (char*)malloc(size+1); // Allocating memory for the response to be stored in the cache element
		memcpy(element->data, data, size);
		element->data[size] = '\0';
        element -> url = (char*)malloc(1+( strlen( url )*sizeof(char)  )); // Allocating memory for the request to be stored in the cache element (as a key)
		strcpy( element -> url, url );
		element->lru_time_track=time(NULL);    // Updating the time_track
        element->next=head; 
        element->len=size;
        head=element;
        cache_size+=element_size;
        temp_lock_val = pthread_mutex_unlock(&lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		//sem_post(&cache_lock);
		// free(data);
		// printf("--\n");
		// free(url);
        return 1;
    }
    return 0;
}
