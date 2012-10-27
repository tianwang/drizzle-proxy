#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <ev.h>


#include <inttypes.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <libdrizzle/drizzle_client.h>
#include <libdrizzle/drizzle_server.h>

#define MAXLEN 1023
#define PORT 3307 
#define ADDR_IP "0.0.0.0"

#define DRIZZLE_FIELD_MAX 32
#define DRIZZLE_RESULT_ROWS 20

#define DEBUG_LOG(function, log) \
do { \
	printf("%s: %s\n", function, log); \
} while(0)

#define DRIZZLE_RETURN_CHECK(__ret, __function, __drizzle) \
do { \
	  if ((__ret) != DRIZZLE_RETURN_OK) \
	    DRIZZLE_RETURN_ERROR(__function, __drizzle); \
} while(0)

#define DRIZZLE_RETURN_ERROR(__function, __drizzle) \
do { \
	  printf(__function ":%s\n", drizzle_error(__drizzle)); \
	  return; \
} while(0)


typedef enum { 
	CON_STATE_CONNECT_SERVER = 0,        /**< A connection to a backend is about to be made */
	CON_STATE_SEND_HANDSHAKE = 1,        /**< A handshake packet is to be sent to a client */
	CON_STATE_READ_AUTH = 2,             /**< An authentication packet is to be read from a client */
	CON_STATE_SEND_AUTH_RESULT = 3,      /**< The result of an authentication attempt is to be sent to a client */
	CON_STATE_READ_QUERY = 4,            /**< COM_QUERY packets are to be read from a client */
	CON_STATE_SEND_QUERY = 5,            /**< send query to backend */
	CON_STATE_READ_QUERY_RESULT = 6,     /**< read query result from backend */
	CON_STATE_SEND_QUERY_RESULT = 7,     /**< Result set packets are to be sent to a client */
	CON_STATE_CLOSE_CLIENT = 8,          /**< The client connection should be closed */
	CON_STATE_SEND_ERROR = 9,            /**< An unrecoverable error occurred */
	CON_STATE_CLOSE_SERVER = 10,          /**< The server connection should be closed */
} client_con_state_st;

typedef struct
{
    drizzle_con_st con;
	drizzle_result_st result;
	drizzle_column_st column;
    client_con_state_st state;
	char *command_buffer;
	size_t command_length;
	drizzle_command_t command;
	struct ev_io recv_watcher;
    struct ev_loop *loop;
} client_con_st;

drizzle_st drizzle;
struct ev_loop *server_loop;
struct ev_io accept_watcher;
struct ev_io recv_watcher;
drizzle_con_st con_listen;
client_con_st client;

int socket_init();
void accept_callback(struct ev_loop *loop, ev_io *w, int revents);
void recv_callback(struct ev_loop *loop, ev_io *w, int revents);
void send_callback(struct ev_loop *loop, ev_io *w, int revents);

static int drizzle_init();
static void drizzle_end();
static drizzle_return_t proxy_init(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_connect_server(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_send_handshake(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_read_auth(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_send_auth_result(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_read_query(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_send_query(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_read_query_result(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t proxy_send_query_result(drizzle_st *drizzle, client_con_st *client);
static drizzle_return_t process_client(drizzle_st *drizzle, client_con_st *client);
ssize_t proxy_unix_send(client_con_st *c, unsigned char *buf, size_t size);
ssize_t proxy_unix_recv(client_con_st *c, unsigned char *buf, size_t size);

//test
static drizzle_return_t proxy_query_result_test(drizzle_st *drizzle, client_con_st *client);
static int drizzle_init()
{
	if (drizzle_create(&drizzle) == NULL) {
		printf("drizzle_create:NULL\n");
		return 1;
	}

	drizzle_add_options(&drizzle, DRIZZLE_FREE_OBJECTS);
	drizzle_set_verbose(&drizzle, DRIZZLE_VERBOSE_CRAZY);

	if (drizzle_con_create(&drizzle, &con_listen) == NULL) {
		printf("drizzle_con_create:NULL\n");
		return 1;
	}

	drizzle_con_add_options(&con_listen, DRIZZLE_CON_LISTEN);
	drizzle_con_set_tcp(&con_listen, ADDR_IP, PORT);
	drizzle_con_add_options(&con_listen, DRIZZLE_CON_MYSQL);

	if (drizzle_con_listen(&con_listen) != DRIZZLE_RETURN_OK) {
		printf("drizzle_con_listen:%s\n", drizzle_error(&drizzle));
		return 1;
	}

	return 0;
}

static void drizzle_end()
{
	drizzle_con_free(&con_listen);
	drizzle_free(&drizzle);
}

static drizzle_return_t proxy_init(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_con_st *con = &client->con;
	client->state = CON_STATE_CONNECT_SERVER;
}

static drizzle_return_t proxy_connect_server(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_con_st *con = &client->con;
	
	/* Handshake packets. */
	drizzle_con_set_protocol_version(con, 10);
	drizzle_con_set_server_version(con, "libdrizzle example 5.1.47");
	drizzle_con_set_thread_id(con, 1);
	drizzle_con_set_scramble(con, (const uint8_t *)"ABCDEFGHIJKLMNOPQRST");
	//drizzle_con_set_capabilities(con, DRIZZLE_CAPABILITIES_NONE);
	drizzle_con_set_capabilities(con,   DRIZZLE_CAPABILITIES_CLIENT);
	drizzle_con_set_charset(con, 8);
	drizzle_con_set_status(con, DRIZZLE_CON_STATUS_NONE);
	drizzle_con_set_max_packet_size(con, DRIZZLE_MAX_PACKET_SIZE);

	ret = drizzle_handshake_server_write(con);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_handshake_server_write", drizzle);
	client->state = CON_STATE_SEND_HANDSHAKE;

	return ret;
}

static drizzle_return_t proxy_send_handshake(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_con_st *con = &client->con;
	ret = drizzle_handshake_client_read(con);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_handshake_client_read", drizzle);
	client->state = CON_STATE_READ_AUTH;
	return ret;
}

static drizzle_return_t proxy_read_auth(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_con_st *con = &client->con;
	if (drizzle_result_create(con, &client->result) == NULL)
		DRIZZLE_RETURN_ERROR("drizzle_result_create", drizzle);

	ret = drizzle_result_write(con, &client->result, true);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_result_write", drizzle);
	client->state = CON_STATE_SEND_AUTH_RESULT;

	return ret;
}

static drizzle_return_t proxy_send_auth_result(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_command_t command;

	size_t total;
	uint8_t *data = NULL;

	drizzle_con_st *con = &client->con;
	data = (uint8_t *)drizzle_con_command_buffer(con, &command, &total, &ret);
	if (ret == DRIZZLE_RETURN_LOST_CONNECTION ||
			(ret == DRIZZLE_RETURN_OK && command == DRIZZLE_COMMAND_QUIT))
	{
		free(data);
		return;
	}

	DRIZZLE_RETURN_CHECK(ret, "drizzle_con_command_buffer", drizzle);

	if (drizzle_result_create(con, &client->result) == NULL)
		DRIZZLE_RETURN_ERROR("drizzle_result_create", drizzle);

	printf("command: %d, buffer: %s, total: %d\n", command, (char *)data, total);

	client->command = command;
	client->command_buffer = data;
	client->command_length = total;
	client->state = CON_STATE_READ_QUERY;

	return ret;
}

// test
static drizzle_return_t proxy_query_result_test(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	drizzle_result_st *result;
	drizzle_column_st *column;
	drizzle_con_st *con;

	char *field[2];
	char field1[DRIZZLE_FIELD_MAX];
	char field2[DRIZZLE_FIELD_MAX];
	size_t size[2];
	uint64_t x;

	size_t offset;
	size_t len;

	field[0]= field1;
	field[1]= field2;

	con = &client->con;
	result = &client->result;
	column = &client->column;

	drizzle_result_set_column_count(result, 2);

	ret = drizzle_result_write(con, result, false);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_result_write", drizzle);

	/* Columns. */
	if (drizzle_column_create(result, column) == NULL)
		DRIZZLE_RETURN_ERROR("drizzle_column_create", drizzle);

	drizzle_column_set_catalog(column, "default");
	drizzle_column_set_db(column, "drizzle_test_db");
	drizzle_column_set_table(column, "drizzle_test_table");
	drizzle_column_set_orig_table(column, "drizzle_test_table");
	drizzle_column_set_name(column, "test_column_1");
	drizzle_column_set_orig_name(column, "test_column_1");
	drizzle_column_set_charset(column, 8);
	drizzle_column_set_size(column, DRIZZLE_FIELD_MAX);
	drizzle_column_set_type(column, DRIZZLE_COLUMN_TYPE_VARCHAR);

	ret= drizzle_column_write(result, column);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_column_write", drizzle);

	drizzle_column_set_name(column, "test_column_2");
	drizzle_column_set_orig_name(column, "test_column_2");

	ret= drizzle_column_write(result, column);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_column_write", drizzle);

	drizzle_column_free(column);

	drizzle_result_set_eof(result, true);

	ret= drizzle_result_write(con, result, false);
	DRIZZLE_RETURN_CHECK(ret, "drizzle_result_write", drizzle);

	/* Rows. */
	for (x= 0; x < DRIZZLE_RESULT_ROWS; x++)
	{  
		size[0]= (size_t)snprintf(field[0], DRIZZLE_FIELD_MAX,
				"field %" PRIu64 "-1", x);
		if (size[0] >= DRIZZLE_FIELD_MAX)
			size[0]= DRIZZLE_FIELD_MAX - 1;

		size[1]= (size_t)snprintf(field[1], DRIZZLE_FIELD_MAX,
				"field %" PRIu64 "-2", x);
		if (size[1] >= DRIZZLE_FIELD_MAX)
			size[1]= DRIZZLE_FIELD_MAX - 1;

		/* This is needed for MySQL and old Drizzle protocol. */
		drizzle_result_calc_row_size(result, (drizzle_field_t *)field, size);

		ret= drizzle_row_write(result);
		DRIZZLE_RETURN_CHECK(ret, "drizzle_row_write", drizzle);

		/* Fields. */
		ret= drizzle_field_write(result, (drizzle_field_t)field[0], size[0],
				size[0]);
		DRIZZLE_RETURN_CHECK(ret, "drizzle_field_write", drizzle);

		ret= drizzle_field_write(result, (drizzle_field_t)field[1], size[1],
				size[1]);
		DRIZZLE_RETURN_CHECK(ret, "drizzle_field_write", drizzle);
	}

	DEBUG_LOG("proxy_query_result_test", "exit");


	return ret;
}

static drizzle_return_t proxy_read_query(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;

	do {
		if (client->command != DRIZZLE_COMMAND_QUERY) {
			ret = drizzle_result_write(&client->con, &client->result, true);
			DRIZZLE_RETURN_CHECK(ret, "drizzle_result_write", drizzle);
			break;
		}

		if (strcasecmp(client->command_buffer, "show databases")==0 ||
				strcasecmp(client->command_buffer, "show tables")==0) {
			drizzle_result_set_column_count(&client->result, 0);
			ret = drizzle_result_write(&client->con, &client->result, true);
			break;
		}

		ret = proxy_query_result_test(drizzle, client);
		DRIZZLE_RETURN_CHECK(ret, "proxy_query_result_test", drizzle);

		ret = drizzle_result_write(&client->con, &client->result, true);
		DRIZZLE_RETURN_CHECK(ret, "drizzle_result_write", drizzle);

	} while(0);

	return ret;
}

static drizzle_return_t proxy_send_query(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	return ret;
	// query
}

static drizzle_return_t proxy_read_query_result(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	return ret;
	// query
}

static drizzle_return_t proxy_send_query_result(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret;
	return ret;
	// query
}

static drizzle_return_t process_client(drizzle_st *drizzle, client_con_st *client)
{
	drizzle_return_t ret; 
	switch(client->state) {
		case CON_STATE_CONNECT_SERVER:
			ret = proxy_connect_server(drizzle, client);
			break; 
		case CON_STATE_SEND_HANDSHAKE:
			ret = proxy_send_handshake(drizzle, client);
			break;
		case CON_STATE_READ_AUTH: 
			ret = proxy_read_auth(drizzle, client);
			break;
		case CON_STATE_SEND_AUTH_RESULT:
			ret = proxy_send_auth_result(drizzle, client); 
			break;
		case CON_STATE_READ_QUERY:
			ret = proxy_read_query(drizzle, client);
			break;
		case CON_STATE_SEND_QUERY:
			ret = proxy_send_query(drizzle, client);
			break;
		case CON_STATE_READ_QUERY_RESULT:
			ret = proxy_read_query_result(drizzle, client);
			break;
		case CON_STATE_SEND_QUERY_RESULT:
			ret = proxy_send_query(drizzle, client);
			break; 
		default:
			break;
	}

	return ret;
}

int main(int argc ,char** argv)
{
	//thread_init();
	int listen;

	drizzle_init();

	listen = drizzle_con_fd(&con_listen);
	//listen = socket_init();
	server_loop = ev_default_loop(0);
	ev_io_init(&accept_watcher, accept_callback, listen, EV_READ);
	ev_io_start(server_loop, &accept_watcher);
	ev_loop(server_loop, 0);
	ev_loop_destroy(server_loop);

	drizzle_end();

	return 0;
}

int socket_init()
{
	struct sockaddr_in my_addr;
	int listener;

	if ((listener = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		perror("socket");
		exit(1);
	} else {
		printf("SOCKET CREATE SUCCESS! \n");
	}

	int so_reuseaddr = 1;
	setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &so_reuseaddr, sizeof(so_reuseaddr));
	bzero(&my_addr, sizeof(my_addr));
	my_addr.sin_family = PF_INET;
	my_addr.sin_port = htons(PORT);
	my_addr.sin_addr.s_addr = inet_addr(ADDR_IP);

	if (bind(listener, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) { 
		perror("bind error!\n");
		exit(1);
	} else { 
		printf("IP BIND SUCCESS,IP:%s\n", ADDR_IP);
	}

	if (listen(listener, 1024) == -1) { 
		perror("listen error!\n");
		exit(1);
	} else { 
		printf("LISTEN SUCCESS,PORT:%d\n", PORT);
	}

	return listener;
}

void accept_callback(struct ev_loop *loop, ev_io *w, int revents)
{
	int newfd;
	drizzle_return_t drizzle_ret;

	//drizzle_con_st *con = NULL;
	//con = (drizzle_con_st*)malloc(sizeof(drizzle_con_st));
	DEBUG_LOG("accept_callback", "enter");

	(void)drizzle_con_accept(&drizzle, &client.con, &drizzle_ret);
	if (drizzle_ret != DRIZZLE_RETURN_OK) {
		printf("drizzle_con_accept:%s\n", drizzle_error(&drizzle));
	}


	struct sockaddr_in sin;
	bzero(&sin, sizeof(sin));
	sin.sin_family = PF_INET;
	sin.sin_port = htons(drizzle_con_src_port(&client.con));
	sin.sin_addr.s_addr = inet_addr(drizzle_con_src_host(&client.con));

	newfd = drizzle_con_fd(&client.con);

	printf("accept callback: fd=%d, addr=%s, port=%d\n", newfd, (char*)inet_ntoa(sin.sin_addr), sin.sin_port);

	recv_watcher.fd = newfd;
	recv_watcher.data = &client;
	ev_io_init(&recv_watcher, send_callback, newfd, EV_WRITE);
	ev_io_start(server_loop, &recv_watcher);

	DEBUG_LOG("accept_callback", "exit");
}

void recv_callback(struct ev_loop *loop, ev_io *w, int revents)
{
	DEBUG_LOG("recv_callback", "enter");

	int ret = 0;
	client_con_st *client;

	client = (client_con_st *)w->data;
	drizzle_con_set_revents(&client->con, POLLIN);
	process_client(&drizzle, client);

	int fd = w->fd;
	ev_io_stop(loop,  w);
	ev_io_init(w, send_callback, fd, EV_WRITE);
	ev_io_start(loop, w);
	printf("thread[%lu] socket fd : %d, turn read 2 write loop! ", pthread_self(), fd);

	DEBUG_LOG("recv_callback", "exit");
}

void send_callback(struct ev_loop *loop, ev_io *w, int revents)
{
	DEBUG_LOG("send_callback", "enter");

	int ret = 0;
	ssize_t n;
	client_con_st *client;

	client = (client_con_st *)w->data;
	drizzle_con_set_revents(&client->con, POLLOUT);
	process_client(&drizzle, client);

	int fd = w->fd;
	ev_io_stop(loop, w);
	ev_io_init(w, recv_callback, fd, EV_READ);
	ev_io_start(loop, w);

	DEBUG_LOG("send_callback", "exit");
}

ssize_t proxy_unix_send(client_con_st *c, unsigned char *buf, size_t size)
{
	ssize_t n;
	int     err;
	int fd;

	for ( ;; ) {
		n = send(fd, buf, size, 0);
		// todo: debug log

		if (n > 0) {
			if (n < (ssize_t) size) {
				// todo
			}

			return n;
		}

		err = errno;

		if (n == 0) {
			// todo: log error
			return n;
		}

		if (err == EAGAIN || err == EINTR) {
			// todo: debug log
			if (err == EAGAIN) {
				return EAGAIN; // error
			}
		} else {
			// todo: log error
			return 1; 
		}
	}
}

ssize_t proxy_unix_recv(client_con_st *c, unsigned char *buf, size_t size)
{
	ssize_t       n;
	int           err;
	int           fd;

	do {
		n = recv(fd, buf, size, 0);
		// log

		if (n == 0) {
			// connection close
			// todo: log
			return n;

		} else if (n > 0) {

			if ((size_t) n < size) {
				// todo
			}

			return n;
		}

		err = errno;

		if (err == EAGAIN || err == EINTR) {
			// Resource temporarily unavailable, try recv again
			// todo: log
			n = (ssize_t)EAGAIN;
		} else {
			// connection exception, need close fd 
			// todo: log
			break;
		}

	} while (err == EINTR);

	return n;
}
