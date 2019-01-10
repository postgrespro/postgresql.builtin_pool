#include <pthread.h>
#include <unistd.h>
#include <errno.h>

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "libpq/libpq.h"
#include "libpq/libpq-be.h"
#include "postmaster/postmaster.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/memutils.h"

#define INIT_BUF_SIZE      (64*1024)
#define MAX_READY_EVENTS   128
#define DB_HASH_SIZE       101
#define PROXY_WAIT_TIMEOUT 1000 /* 1 second */

struct SessionPool;
struct Proxy;

typedef struct
{
	char database[NAMEDATALEN];
	char username[NAMEDATALEN];
}
SessionPoolKey;

typedef struct Channel
{
	char*    buf;
	int      rx_pos;
	int      tx_pos;
	int      tx_size;
	int      buf_size;
	int      event_pos;          /* Position of wait event returned by AddWaitEventToSet */

	Port*    client_port;        /* Not null for client, null for server */

	pgsocket backend_socket;
	PGPROC*  backend_proc;
	int      backend_pid;
	bool     backend_is_tainted;
	bool     backend_is_ready;   /* ready for query */
	bool     is_disconnected;

	bool     skip_handshake_response;
	int      handshake_response_size;
	char*    handshake_response;

	struct Channel* peer;
	struct Channel* next;
	struct Proxy*   proxy;
	struct SessionPool* pool;
}
Channel;

typedef struct Proxy
{
	MemoryContext memctx;        /* Memory context for this proxy (used only in single thread) */
	MemoryContext tmpctx;        /* Temporary memory context used for parsing startup packet */
	WaitEventSet* wait_events;   /* Set of socket descriptors of backends and clients socket descriptors */
	HTAB*    pools;              /* Session pool map with dbname/role used as a key */
	int      n_accepted_connections; /* Number of accepted but not yet established connections
									  * (startup packeg is not received and db/role are not known) */
	int      max_backends;       /* Maximal number of backends per database */
	int      n_pools;            /* Number of dbname/role combinations */
	int      n_clients;          /* Total number of clients */
	int      n_backends;         /* Total number of backends */
	bool     shutdown;           /* Shutdown flag */
	Channel* hangout;            /* List of disconncted backends */
	pthread_t thread;
} Proxy;

typedef struct SessionPool
{
	SessionPoolKey key;
	Channel* idle_backends;
	Channel* pending_clients;
	Proxy*   proxy;
	void*    startup_packet;      /* startup packet for this pool */
	int      startup_packet_size; /* startup packet size */
	int      n_launched_backends; /* Total number of launched backends */
	int      n_idle_backends;     /* Number of backends in idle state */
	int      n_tainted_backends;  /* Number of backends with pinned sessions */
	int      n_connected_clients; /* Total number of connected clients */
	int      n_idle_clients;      /* Number of clients in idle state */
	int      n_pending_clients;   /* Number of clients waiting for free backend */
}
SessionPool;

static pthread_mutex_t postmaster_mutex = PTHREAD_MUTEX_INITIALIZER;

static void channel_remove(Channel* chan);
static Channel* backend_start(SessionPool* pool, Port* client_port);
static bool channel_read(Channel* chan);
static bool channel_write(Channel* chan, bool synchronous);

//#define ELOG(severity, fmt,...) do { postmaster_lock(); elog(severity, "PROXY: " fmt, ## __VA_ARGS__); postmaster_unlock(); } while (0)
#define ELOG(severity,fmt,...)

/**
 * Backend is ready for next command outside transaction block (idle state).
 * Now if backebd is not tainted it is possible to schedule some other client to this backend
 */
static bool
backend_reschedule(Channel* chan)
{
	chan->backend_is_ready = false;
	if (chan->backend_proc == NULL) /* Lazy resolving of PGPROC entry */
	{
		/*
		 * Postmaster has no PGPROC entry so it can ont wait, so we have to use onn-sycnhronized version BackendPidGetProc.
		 * Hoping that race condition here is not critical.
		 */
		Assert(chan->backend_pid != 0);
		chan->backend_proc = BackendPidGetProcWithLock(chan->backend_pid);
		Assert(chan->backend_proc); /* If backend completes execution of some query, then it has definitely registered itself in procarray */
	}
	if (!chan->backend_proc->is_tainted) /* If backend is not storing some session context */
	{
		Channel* pending = chan->pool->pending_clients;
		Assert(!chan->backend_is_tainted);
		chan->peer->peer = NULL;
		chan->pool->n_idle_clients += 1;
		if (pending)
		{
            /* Has pending clients: serve one of them */
			ELOG(LOG, "Backed %d is reassigned to client %p", chan->backend_pid, pending);
			chan->pool->pending_clients = pending->next;
			chan->peer = pending;
			pending->peer = chan;
			chan->pool->n_pending_clients -= 1;
			if (pending->tx_size == 0) /* new client has sent startup packet and we now need to send handshake response */
			{
				Assert(chan->handshake_response != NULL); /* backend already send handshake response */
				Assert(chan->handshake_response_size < chan->buf_size);
				memcpy(chan->buf, chan->handshake_response, chan->handshake_response_size);
				chan->rx_pos = chan->tx_size = chan->handshake_response_size;
				ELOG(LOG, "Simulate response for startup packet to client %p", pending);
				return channel_write(pending, false);
			}
			else
			{
				ELOG(LOG, "Try to send pending request from client %p to backend %p (pid %d)", pending, chan, chan->backend_pid);
				Assert(pending->tx_pos == 0 && pending->rx_pos >= pending->tx_size);
				return channel_write(chan, false); /* Send pending request to backend */
			}
		}
		else /* return backend to the list of idle backends */
		{
			ELOG(LOG, "Backed %d is idle", chan->backend_pid);
			chan->next = chan->pool->idle_backends;
			chan->pool->idle_backends = chan;
			chan->pool->n_idle_backends += 1;
			chan->peer = NULL;
		}
	}
	else if (!chan->backend_is_tainted) /* if it was not marked as tainted before... */
	{
		chan->backend_is_tainted = true;
		chan->pool->n_tainted_backends += 1;
	}
	return true;
}

/**
 * Parse client's startup packet and assign client to proper connection pool based on dbname/role
 */
static bool
client_connect(Channel* chan, int startup_packet_size)
{
	bool found;
	SessionPoolKey key;
	char* startup_packet = chan->buf;

	Assert(chan->client_port);

	MemoryContextReset(chan->proxy->tmpctx);
	MemoryContextSwitchTo(chan->proxy->tmpctx);
	MyProcPort = chan->client_port;
	if (ParseStartupPacket(chan->client_port, chan->proxy->tmpctx, startup_packet+4, startup_packet_size-4, false) != STATUS_OK) /* skip packet size */
	{
		ELOG(WARNING, "Failed to parse startup packet for client %p", chan);
		return false;
	}
	pg_set_noblock(chan->client_port->sock);
	memset(&key, 0, sizeof(key));
	strlcpy(key.database, chan->client_port->database_name, NAMEDATALEN);
	strlcpy(key.username, chan->client_port->user_name, NAMEDATALEN);

	ELOG(LOG, "Client %p connects to %s/%s", chan, key.database, key.username);

	chan->pool = (SessionPool*)hash_search(chan->proxy->pools, &key, HASH_ENTER, &found);
	if (!found)
	{
		chan->proxy->n_pools += 1;
		memset((char*)chan->pool + sizeof(SessionPoolKey), 0, sizeof(SessionPool) - sizeof(SessionPoolKey));
		chan->pool->startup_packet = malloc(startup_packet_size);
		memcpy(chan->pool->startup_packet, startup_packet, startup_packet_size);
		*(ProtocolVersion *)chan->pool->startup_packet = PG_PROTOCOL_LATEST; /* disable SSL negotiation request */
		chan->pool->startup_packet_size = startup_packet_size;
	}
	chan->pool->proxy = chan->proxy;
	chan->pool->n_connected_clients += 1;
	chan->pool->n_idle_clients += 1;
	chan->proxy->n_accepted_connections -= 1;
	return true;
}

/*
 * Attach client to backend. Return true if new backend is attached, false otherwise.
 * It is necessary to send startup packet only to new backend.
 */
static bool
client_attach(Channel* chan)
{
	Channel* idle_backend = chan->pool->idle_backends;
	chan->pool->n_idle_clients -= 1;
	if (idle_backend)
	{
		/* has idle backend */
		Assert(!idle_backend->backend_is_tainted);
		chan->peer = idle_backend;
		idle_backend->peer = chan;
		chan->pool->idle_backends = idle_backend->next;
		chan->pool->n_idle_backends -= 1;
		ELOG(LOG, "Attach client %p to backend %p (pid %d)", chan, idle_backend, idle_backend->backend_pid);
	}
	else /* all backends are busy */
	{
		if (chan->pool->n_launched_backends < chan->proxy->max_backends)
		{
			/* Try to start new backend */
			idle_backend = backend_start(chan->pool, chan->client_port);
			if (idle_backend != NULL)
			{
				ELOG(LOG, "Start new backend %p (pid %d) for client %p",
					 idle_backend, idle_backend->backend_pid, chan);
				chan->peer = idle_backend;
				idle_backend->peer = chan;
				return true; /* send startup packet to new backend */
			}
		}
		/* Wait until some backend is available */
		ELOG(LOG, "Client %p is waiting for available backends", chan);
		chan->next = chan->pool->pending_clients;
		chan->pool->pending_clients = chan;
		chan->pool->n_pending_clients += 1;
	}
	return false;
}

/*
 * Handle communication failure for this channel.
 * It is not possible to remove channel immediately because it can be triggered by other epoll events.
 * So link all channels in L1 list for pending delete.
 */
static void
channel_hangout(Channel* chan, char const* op)
{
	if (chan->is_disconnected)
	   return;

	if (chan->client_port) {
		ELOG(LOG, "Hangout client %p due to %s error: %m", chan, op);
	} else {
		ELOG(LOG, "Hangout backend %p (pid %d) due to %s error: %m", chan, chan->backend_pid, op);
	}
	chan->next = chan->proxy->hangout;
	chan->proxy->hangout = chan;
	chan->is_disconnected = true;
	chan->backend_is_ready = false;
   	if (chan->client_port && chan->peer)
	{
		/* detach backend from client */
		backend_reschedule(chan->peer);
	}
#if 0
	if (chan->peer && !chan->peer->is_disconnected)
		channel_hangout(chan->peer, "peer");
#endif
}

/*
 * Try to send some data to the channel.
 * Data is located in peer buffer. Because of using edge-triggered mode we have have to use non-blocking IO
 * and try to write all avaialble data. Once write is completed we should try to read more data from source socket.
 * "sycnhronous" flag is used to avoid infinite recursion or reads-writers.
 * Returns true if there is nothing to do or operation is succeffully completed, false in case of error
 * or socket buffer is full.
 */
static bool
channel_write(Channel* chan, bool synchronous)
{
	Channel* peer = chan->peer;

	if (peer == NULL)
		return false;

	while (peer->tx_pos < peer->tx_size) /* has something to write */
	{
		ssize_t rc = chan->client_port
			? secure_raw_write(chan->client_port, peer->buf + peer->tx_pos, peer->tx_size - peer->tx_pos)
			: write(chan->backend_socket, peer->buf + peer->tx_pos, peer->tx_size - peer->tx_pos);
		ELOG(LOG, "%p: write %d tx_pos=%d, tx_size=%d: %m", chan, (int)rc, peer->tx_pos, peer->tx_size);
		if (rc <= 0)
		{
			if (rc == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
				channel_hangout(chan, "write");
			return false;
		}
		peer->tx_pos += rc;
	}
	if (peer->tx_size != 0)
	{
		chan->backend_is_ready = false;
		Assert(peer->rx_pos >= peer->tx_size);
		memmove(peer->buf, peer->buf + peer->tx_size, peer->rx_pos - peer->tx_size);
		peer->rx_pos -= peer->tx_size;
		peer->tx_pos = peer->tx_size = 0;
	}
	//	channel_read(chan);
	return synchronous || channel_read(peer); /* write is not invoked from read */
}

/*
 * Try to read more data from the channel and send it to the peer.
 */
static bool
channel_read(Channel* chan)
{
	int  msg_start;
	while (chan->tx_size == 0) /* there is no pending write op */
	{
		ssize_t rc = chan->client_port
			? secure_raw_read(chan->client_port, chan->buf + chan->rx_pos, chan->buf_size - chan->rx_pos)
			: read(chan->backend_socket, chan->buf + chan->rx_pos, chan->buf_size - chan->rx_pos);

		ELOG(LOG, "%p: read %d: %m", chan, (int)rc);
		if (rc <= 0)
		{
			if (rc == 0 || (errno != EAGAIN && errno != EWOULDBLOCK))
				channel_hangout(chan, "read");
			return false; /* wait for more data */
		}
		chan->rx_pos += rc;
		msg_start = 0;
		while (chan->rx_pos - msg_start >= 5) /* have message code + length */
		{
			int msg_len;
			bool handshake = false;
			if (chan->pool == NULL) /* process startup packet */
			{
				Assert(msg_start == 0);
				memcpy(&msg_len, chan->buf + msg_start, sizeof(msg_len));
				msg_len = ntohl(msg_len);
				handshake = true;
			}
			else
			{
				ELOG(LOG, "%p receive message %c", chan, chan->buf[msg_start]); 
				memcpy(&msg_len, chan->buf + msg_start + 1, sizeof(msg_len));
				msg_len = ntohl(msg_len) + 1;
			}
			if (msg_start + msg_len > chan->buf_size)
			{
				chan->buf_size = msg_start + msg_len;
				chan->buf = realloc(chan->buf, chan->buf_size);
			}
			if (chan->rx_pos - msg_start >= msg_len) /* Message is completely fetched */
			{
				int response_size = msg_start + msg_len;
				if (chan->pool == NULL) /* receive startup packet */
				{
					Assert(chan->client_port);
					if (!client_connect(chan, msg_len))
					{
						chan->is_disconnected = true;
						channel_remove(chan);
						return false;
					}
				}
				else if (!chan->client_port /* message from backend */
					&& chan->buf[msg_start] == 'Z'  /* ready for query */
					&& chan->buf[msg_start+5] == 'I') /* Transaction block status is idle */
				{
					if (chan->handshake_response == NULL)
					{
						/* Save handshake response */
						chan->handshake_response_size = response_size;
						chan->handshake_response = malloc(response_size);
						memcpy(chan->handshake_response, chan->buf, response_size);
					}
					if (chan->skip_handshake_response)
					{
						memmove(chan->buf, chan->buf + response_size, chan->rx_pos -= response_size);
						chan->skip_handshake_response = false;
						msg_start = 0;
						continue;
					}
					Assert(chan->rx_pos - msg_start == msg_len); /* should be last message */
					chan->backend_is_ready = true;
				}
				else if (chan->client_port /* message from client */
						 && chan->buf[msg_start] == 'X')	/* terminate message */
				{
					if (chan->peer == NULL || !chan->peer->backend_is_tainted)
					{
						/* Skip terminate message to idle and non-tinted backends */
						channel_hangout(chan, "terminate");
						return false;
					}
				}
				if (chan->peer == NULL)  /* client is not yet connected to backend */
				{
					if (!chan->client_port)
					{
						/* We are not expecting messages from idle backend. Assume that it some error or shutdown. */
						channel_hangout(chan, "idle");
						return false;
					}
					if (client_attach(chan)) /* new backend requires startup packet */
					{
						if (!handshake) /* if it is existed client, then copy startup packet from pool */
						{
							if (response_size + chan->pool->startup_packet_size > chan->buf_size)
							{
								chan->buf_size = response_size + chan->pool->startup_packet_size;
								chan->buf = realloc(chan->buf, chan->buf_size);
							}
							memmove(chan->buf + chan->pool->startup_packet_size, chan->buf, chan->rx_pos);
							memcpy(chan->buf, chan->pool->startup_packet, chan->pool->startup_packet_size);
							msg_start += chan->pool->startup_packet_size;
							chan->rx_pos += chan->pool->startup_packet_size;
							chan->peer->skip_handshake_response = true;
						}
					}
					else if (handshake) /* Do not need to send startup packet to reused backend,
										 * but we need to send hasndshale response to the client */
					{
						Channel* backend = chan->peer;
						Assert(chan->rx_pos == msg_len && msg_start == 0);
						chan->rx_pos = 0; /* skip startup packet */
						if (backend != NULL) /* if backend was assigned */
						{
							Assert(backend->handshake_response != NULL); /* backend already send handshake response */
							Assert(backend->handshake_response_size < backend->buf_size);
							memcpy(backend->buf, backend->handshake_response, backend->handshake_response_size);
							backend->rx_pos = backend->tx_size = backend->handshake_response_size;
							return channel_write(chan, false);
						}
						else
						{
							/* dummy handshake response will be send to client later when backen is assiged */
							return false;
						}
					}
					else if (chan->peer == NULL) /* backend was not assigned */
					{
						chan->tx_size = response_size; /* query will be send later once backend is assigned */
						return false;
					}
				}
				msg_start += msg_len;
			}
			else break;
		}
		if (msg_start != 0)
		{
			/* has some complete messages to send to peer */
			Assert(chan->tx_pos == 0);
			Assert(chan->rx_pos >= msg_start);
			chan->tx_size = msg_start;
			if (!channel_write(chan->peer, true))
				return false;
		}
		if (chan->backend_is_ready)
			return backend_reschedule(chan);
	}
	return true;
}

static Channel*
channel_create(Proxy* proxy)
{
	Channel* chan = (Channel*)calloc(1, sizeof(Channel));
	chan->proxy = proxy;
	chan->buf = malloc(INIT_BUF_SIZE);
	chan->buf_size = INIT_BUF_SIZE;
	chan->tx_pos = chan->rx_pos = chan->tx_size = 0;
	return chan;
}

static bool
channel_register(Proxy* proxy, Channel* chan)
{
	pgsocket sock = chan->client_port ? chan->client_port->sock : chan->backend_socket;
	pg_set_noblock(sock);
	chan->event_pos =
		AddWaitEventToSet(proxy->wait_events, WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE|WL_SOCKET_EDGE,
						  sock, NULL, chan);
	if (chan->event_pos < 0)
	{
		elog(WARNING, "PROXY: Failed to add new client - too much sessions: %d clients, %d backends. "
					 "Try to increase 'max_sessions' configuration parameter.",
					 proxy->n_clients, proxy->n_backends);
		return false;
	}
	return true;
}

/*
 * Start new backend for particular pool associated with dbname/role combination.
 * Backend is forked using BackendStartup function.
 * This function is called from proxy thread and access static variabls of backend.c,
 * so use mutex to prevent race condition.
 */
static Channel*
backend_start(SessionPool* pool, Port* client_port)
{
	int socks[2];
	int rc, pid;
	Port* port = (Port*)calloc(1, sizeof(Port));
	Channel* chan = NULL;

	socketpair(AF_UNIX, SOCK_STREAM, 0, socks);
	port->sock = socks[0];
	port->laddr = client_port->laddr;
	port->raddr = client_port->raddr;

	postmaster_lock();
	set_stack_base();
	rc = BackendStartup(port, &pid);
	free(port);

	if (rc == STATUS_OK)
	{
		chan = channel_create(pool->proxy);
		close(socks[0]); /* not needed in parent process */
		chan->pool = pool;
		chan->backend_socket = socks[1];
		chan->backend_pid = pid;
		if (channel_register(pool->proxy, chan))
		{
			pool->proxy->n_backends += 1;
			pool->n_launched_backends += 1;
		}
		else
		{
			free(chan->buf);
			free(chan);
			close(socks[1]);
			chan = NULL;
		}
	}
	else
	{
		elog(WARNING, "Failed to start backend: %d", rc);
	}
	postmaster_unlock();
	return chan;
}

/*
 * Add new client, accepted by postmaster. This client will be assigned to concrete session pool
 * when it's startup packet is received.
 */
void
proxy_add_client(Proxy* proxy, Port* port)
{
	Channel* chan = channel_create(proxy);
	chan->client_port = port;
	chan->backend_socket = PGINVALID_SOCKET;
	if (channel_register(proxy, chan)) {
		elog(LOG, "Add new client %p", chan);
		proxy->n_accepted_connections += 1;
		proxy->n_clients += 1;
	}
}

/*
 * Perform delayed deletion of channel
 */
static void
channel_remove(Channel* chan)
{
	Assert(chan->is_disconnected); /* should be marked as disconnected by channel_hangout */
	postmaster_lock();
	DeleteWaitEventFromSet(chan->proxy->wait_events, chan->event_pos);
	if (chan->client_port)
	{
		if (chan->pool)
			chan->pool->n_connected_clients -= 1;
		else
			chan->proxy->n_accepted_connections -= 1;
		chan->proxy->n_clients -= 1;
		close(chan->client_port->sock);
		free(chan->client_port);
	}
	else
	{
		chan->proxy->n_backends -= 1;
		chan->pool->n_launched_backends -= 1;
		close(chan->backend_socket);
		free(chan->handshake_response);
	}
	free(chan->buf);
	free(chan);
	postmaster_unlock();
}

/*
 * Create new proxy.
 */
Proxy*
proxy_create(int max_backends)
{
	HASHCTL ctl;
	Proxy*  proxy = calloc(1, sizeof(Proxy));
	proxy->memctx = AllocSetContextCreate(TopMemoryContext,
										  "Proxy",
										  ALLOCSET_DEFAULT_SIZES);
	proxy->tmpctx = AllocSetContextCreate(proxy->memctx,
										  "Startup packet parsing context",
										  ALLOCSET_DEFAULT_SIZES);
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(SessionPoolKey);
	ctl.entrysize = sizeof(SessionPool);
	ctl.hcxt = proxy->memctx;
	proxy->pools = hash_create("Pool by database and user", DB_HASH_SIZE,
							   &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	proxy->wait_events = CreateWaitEventSet(TopMemoryContext, MaxSessions*2); /* we need events both for clients and backends */
	proxy->max_backends = max_backends;
	return proxy;
}

/*
 * Lock mutex to avoid race condition with postmaster
 */
int
postmaster_lock(void)
{
	int rc = pthread_mutex_lock(&postmaster_mutex);
	Assert(rc == 0);
	return rc;
}

/*
 * Unlock mutex
 */
int
postmaster_unlock(void)
{
	int rc = pthread_mutex_unlock(&postmaster_mutex);
	return rc;
}

/*
 * Main proxy loop
 */
static void*
proxy_loop(void* arg)
{
	Proxy* proxy = (Proxy*)arg;

	int i, n_ready;
	WaitEvent ready[MAX_READY_EVENTS];
	Channel *chan, *next;
	while (!proxy->shutdown)
	{
		/* Use timeout to allow normal proxy shutdown */
		n_ready = WaitEventSetWait(proxy->wait_events, PROXY_WAIT_TIMEOUT, ready, MAX_READY_EVENTS, PG_WAIT_CLIENT);
		for (i = 0; i < n_ready; i++) {
			chan = (Channel*)ready[i].user_data;
			if (ready[i].events & WL_SOCKET_WRITEABLE) {
				ELOG(LOG, "Channel %p is writable", chan);
				channel_write(chan, false);
			}
			if (ready[i].events & WL_SOCKET_READABLE) {
				ELOG(LOG, "Channel %p is readable", chan);
				channel_read(chan);
			}
		}
		/* Delayed deallocation of disconnected channels */
		for (chan = proxy->hangout; chan != NULL; chan = next)
		{
			next = chan->next;
			channel_remove(chan);
		}
		proxy->hangout = NULL;
	}
	return NULL;
}

/*
 * Launch proxy thread
 */
void
proxy_start(Proxy* proxy)
{
	pthread_create(&proxy->thread, NULL, proxy_loop, proxy);
}

/*
 * Wait completion of proy thread
 */
void
proxy_stop(Proxy* proxy)
{
	void* status;
	proxy->shutdown = true;
	pthread_join(proxy->thread, &status);
}

/*
 * Get proxy workload: currently just number of clients
 */
int  proxy_work_load(struct Proxy* proxy)
{
	return proxy->n_clients;
}
