#include <pthread.h>

#include "postgres.h"
#include "storage/latch.h"

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
	int      event_pos;

	Port*    client_port;

	pgsocket backend_socket;
	PGPROC*  backend_proc;
	int      backend_pid;
	bool     backend_is_tainted;

	bool     is_disconnected;

	struct Channel* peer;
	struct SessionPool* pool;
	struct Proxy*   proxy;
}
Channel;

typedef struct Proxy
{
	MemoryContext memctx;
	WaitEventSet* wait_events;
	HTAB*    pools;
	int      n_accepted_connections;
	int      max_backends;
	Channel* hangout;
	pthread_t thread;
	bool     shutdown;
} Proxy;

typedef struct SessionPool;
{
	SessionPoolKey key;
	Channel* idle_backends;
	Channel* penging_clients;
	Channel* hangout;
	Proxy*   proxy;
	int      n_launched_backends;
	int      n_idle_backends;
	int      n_tainted_backends;
	int      n_connected_clients; /* total number of connected clients */
	int      n_idle_clients;      /* number of client sin dile state */
	int      n_pending_clients;   /* clients waiting for free backend */
}
SessionPool;

static void channel_remove(Channel* chan);
static Channel* backend_start(SessionPool* pool);

static void
backend_reschedule(Channel* chan)
{
	if (chan->backend_proc == NULL)
	{
		chan->backend_proc = BackendPidGetProcWithLock(pid);
		Assert(chan->backend_proc);
	}
	if (!chan->backend_proc->is_tainted)
	{
		chan->peer->peer = NULL;
		chan->pool->n_idle_clients += 1;
		if (chan->pool->pending_clients)
		{
            /* Has pending clients: serve one of them */
			Channel* pending = chan->pool->pending_clients;
			chan->pool->pending_clients = pending->next;
			chan->peer = pending;
			pending->peer = chan;
			chan->pool->n_pending_clients -= 1;
		}
		else /* return backend to list of idle backends */
		{
			chan->next = chan->pool->idle_backends;
			chan->pool->idle_backends = chan;
			chan->pool->n_idle_backends += 1;
			chan->peer = NULL;
		}
	}
	else if (!chan->backend_is_tainted)
	{
		chan->backend_is_tainted = true;
		chan->pool->n_tainted_backends += 1;
	}
}

static bool
client_connect(Channel* chan, int len)
{
	bool found;
	SessionPoolKey key;
	Assert(chan->client_port);

	if (ParseStartupPacket(chan->client_port, len) != STATUS_OK)
		return false;

	strlcpy(key.database, port->database_name, NAMEDATALEN);
	strlcpy(key.username, port->user_name, NAMEDATALEN);
	chan->pool = (SessioNPool*)hash_search(chan->proxy->pools, &key, HASH_ENTER, &found);
	if (!found)
	{
		memset((char*)chan->pool + sizeof(SessionPoolKey), 0, sizeof(SessionPool) - sizeof(SessionPoolKey));
	}
	chan->pool->n_connected_clients += 1;
	chan->pool->n_idle_clients += 1;
	chan->proxy->n_accepted_connections -= 1;
	return true;
}

static void
client_attach(Channel* chan)
{
	chan->pool->n_idle_clients -= 1;
	if (chan->pool->idle)
	{
		/* has idle backend */
		chan->peer = chan->pool->idle_backends;
		chan->peer->peer = chan;
		chan->pool->idle_backends = chan->pool->idle_backends->next;
		chan->pool->n_idle_backends -= 1;
	}
	else /* all backends are busy */
	{
		if (chan->pool->n_launched_backends < chan->proxy->max_backends)
		{
			/* Try to start new backend */
			Channel* backend = backend_start(pool);
			if (backend != NULL)
			{
				chan->peer = backend;
				backend->peer = chan;
				return;
			}
		}
		/* Wait until some backend is available */
		chan->next = chan->pool->pending_clients;
		chan->pool->pending_clients = chan;
		chan->pool->n_pending_clients += 1;
	}
	chan->rx_pos = 0; /* skip startup package for  existed backend */
}

static void
channel_hangout(Channel* chan)
{
	Assert(!chan->is_disconnected);
	chan->next = chan->proxy->hangout;
	chan->proxy->hangout = chan;
	chan->is_disconnected = true;
	if (chan->peer && !chan->peer->is_disconnected)
		channel_hangout(chan->peer)
}

static void
channel_write(Channel* chan, bool synchronous)
{
	Channel* peer = chan->peer;

	while (peer->tx_pos < peer->tx_size) /* has something to write */
	{
		ssize_t rc = chan->client_port
			? secure_write(chan->client_port, peer->buf + peer->tx_pos, chan->tx_size - peer->tx_pos)
			: write(chan->backend_socket, peer->buf + peer->tx_pos, chan->tx_size - peer->tx_pos);

		if (rc <= 0)
		{
			if (errno != EAGAIN && errno != EWOULDBLOCK)
				channel_hangout(chan);
			return false;
		}
		peer->tx_pos += rc;
		if (peer->tx_pos == peer->tx_size) /* message is completely written */
		{
			memmove(peer->buf, peer->buf + peer->tx_size, peer->rx_pos - peer->tx_size);
			peer->rx_pos -= peer->tx_size;
			peer->tx_pos = peer->tx_size = 0;
			return synchronous || channel_read(peer);
		}
	}
	return true;
}

static bool
channel_read(Channel* chan)
{
	while (chan->tx_size == 0) /* there is no pending write op */
	{
		ssize_t rc = chan->client_port
			? secure_read(chan->client_port, chan->buf + chan->rx_pos, chan->buf_size - chan->rx_pos)
			: read(chan->backend_socket, chan->buf + chan->rx_pos, chan->buf_size - chan->rx_pos);

		if (rc < 0)
		{
			if (errno != EAGAIN && errno != EWOULDBLOCK)
				channel_hangout(chan);
			return false; /* wait for more data */
		}
		if (chan->rx_pos >= 5) /* have message code + length */
		{
			int msg_len;
			if (chan->pool == NULL) /* process startup packet */
			{
				memcpy(&msg_len, chan->buf, sizeof(msg_len));
				msg_len = ntohl(msg_len);
			}
			else
			{
				memcpy(&msg_len, chan->buf+1, sizeof(msg_len));
				msg_len = ntohl(msg_len) + 1;
			}
			if (msg_len > chan->buf_size)
			{
				chan->buf_size = msg_len;
				chan->buf = repalloc(chan->buf, chan->buf_size);
			}
			if (chan->rx_pos >= msg_len) /* Message is completely fetched */
			{
				if (chan->pool == NULL) /* receive startup packet */
				{
					Assert(chan->client_port);
					if (!client_connect(chan, msg_len - 4))
					{
						chan->is_disconnected = true;
						channel_remove(chan);
						return false;
					}
				}
				else if (!chan->client_port      /* message from backend */
					&& chan->buf[0] == 'Z'  /* ready for query */
					&& chan->buf[5] == 'I') /* Transaction block status is idle */
				{
					backend_reschedule(chan);
				}
				if (chan->peer == NULL)
				{
					Assert(chan->client_port); /* client is not yet connected to backend */
					client_attach(chan);
				}
				if (chan->rx_pos) /* chan->rx_pos cab be zero is startup package is skipped */
				{
					chan->tx_pos = msg_len;
					if (!channel_write(chan, true))
						return false;
				}
			}
		}
	}
	return true;
}

static Channel*
backend_start(SessionPool* pool)
{
	int socks[2];
	int rc, pid;
	Port* port = (Port*)calloc(sizeof(Port), 1);

	socketpair(AF_UNIX, SOCK_STREAM, 0, socks);
	port->sock = socks[0];
	rc = BackendStartup(port, &pid);
	if (rc == STATUS_OK)
	{
		Channel* chan = (Channel*)MemoryContextAllocZero(pool->proxy->memctx, sizeof(Channel));
		free(port);
		close(socks[0]); /* not needed */
		pool->n_launched_backends += 1;
		chan->proxy = pool->proxy;
		chan->client_port = NULL;
		chan->backend_socket = socks[1];
		chan->buf = MemoryContextAlloc(pool->proxy->memctx, INIT_BUF_SIZE);
		chan->buf_size = INIT_BUF_SIZE;
		chan->tx_pos = chan->rx_pos = chan->tx_size = 0;
		chan->pool = pool;
		chan->peer = NULL;
		chan->backend_pid = pid;
		chan->backend_proc = NULL; /* not known at this moment */
		chan->backend_is_tainted = false;
		chan->is_disconnected = false;
		pool->n_launched_backends += 1;
		pool->n_idle_backends += 1;
		chan->next = pool->idle_backends;
		pool->idle_backends = chan;
		chan->event_pos =
			AddWaitEventToSet(poll->proxy->wait_events, WL_SOCKET_READABLE|WL_SOCKET_WRITABLE|WL_SOCKE_EDGE,
							  chan->backend_socket, NULL, chan);
		pg_set_noblock(chan->backend_socket);
		return chan;
	}
	return NULL;
}


void
proxy_add_client(Proxy* proxy, pgsocket sock)
{
	Port* port = MemoryContextAllocZero(proxy->memctx, sizeof(Port));
	Channel* chan = (Channel*)MemoryContextAllocZero(proxy->memctx, sizeof(Channel));
	port->sock = sock;
	chan->proxy = proxy;
	chan->client_port = port;
	chan->backend_socket = PGINVALID_SOCKET;
	chan->buf = MemoryContextAlloc(proxy->memctx, INIT_BUF_SIZE);
	chan->buf_size = INIT_BUF_SIZE;
	chan->tx_pos = chan->rx_pos = chan->tx_size = 0;
	chan->pool = NULL; /* not know before receiving startup package */
	chan->peer = NULL;
	chan->backend_proc = NULL;
	chan->backend_is_tainted = false;
	chan->is_disconnected = false;
	proxy->n_accepted_connections += 1;
	chan->event_pos =
		AddWaitEventToSet(proxy->wait_events, WL_SOCKET_READABLE|WL_SOCKET_WRITABLE|WL_SOCKE_EDGE,
						  sock, NULL, chan);
	pg_set_noblock(sock);
}

static void
channel_remove(Channel* chan)
{
	Assert(chan->is_disconnected);
	DeleteWaitEventFromSet(proxy_wait_events, chan->event_pos);
	if (chan->client_port)
	{
		if (chan->pool)
			chan->pool->n_connected_clients -= 1;
		else
			chan->proxy->n_accepted_connections -= 1;
		close(chan->client_port->sock);
	}
	else
	{
		close(chan->backend_socket);
		chan->pool->n_launched_backends -= 1;
	}
	pfree(chan->buf);
	pfree(chan);
}

Proxy*
proxy_create(int max_backends)
{
	HASHCTL		ctl;
	Proxy* proxy = calloc(sizeof(Proxy));
	proxy->memctx = AllocSetContextCreate(TopMemoryContext,
										  "Proxy",
										  ALLOCSET_DEFAULT_SIZES);
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(SessionPoolKey);
	ctl.entrysize = sizeof(SessionPool);
	ctl.hcxt = proxy->memctx;
	proxy->pools = hash_create("Pool by database and user", DB_HASH_SIZE,
							   &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	proxy->wait_events = CreateWaitEventSet(proxy->memctx, MaxSessions);
	proxy->max_backends = max_backends;
	return proxy;
}

static void*
proxy_loop(void* arg)
{
	Proxy* proxy = (Proxy*)arg;

	int i, n_ready;
	WaitEvent ready[MAX_READY_EVENTS];
	Channel *chan, *next;

	while (!proxy->shutdown)
	{
		n_ready = WaitEventSetWait(proxy->wait_events, PROXY_WAIT_TIMEOUT, ready, MAX_READY_EVENTS, PG_WAIT_CLIENT);
		for (i = 0; i < n_ready; i++) {
			chan = (Channel*)proxy->wait_events[i].user_data;
			if (ready[i].events & WL_SOCKET_WRITEABLE) {
				channel_write(chan, false);
			}
			if (ready[i].events & WL_SOCKET_READABLE) {
				channel_read(chan);
			}
		}
		for (chan = proxy->hangout; proxy != NULL; proxy = next)
		{
			next = chan->next;
			channel_remove(chan);
		}
		proxy->hangout = NULL;
	}
}

void
proxy_start(Proxy* proxy)
{
	pthread_create(&proxy->thread, NULL, proxy_start, proxy);
}


void
proxy_stop(Proxy* proxy)
{
	void* status;
	proxy->shutdown = true;
	pthread_join(proxy->thread, &status);
}
