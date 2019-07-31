/*-------------------------------------------------------------------------
 *
 * proxy.h
 *	  Exports from postmaster/proxy.c.
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/postmaster/proxy.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _PROXY_H
#define _PROXY_H

/*
 * Information in share dmemory about connection proxy state (used for session scheduling and monitoring)
 */
typedef struct ConnectionProxyState
{
	int pid;                  /* proxy worker pid */
	int n_clients;            /* total number of clients */
	int n_ssl_clients;        /* number of clients using SSL connection */
	int n_pools;              /* nubmer of dbname/role combinations */
	int n_backends;           /* totatal number of launched backends */
	int n_dedicated_backends; /* number of tainted backends */
	int n_idle_backends;      /* number of idle backends */
	int n_idle_clients;       /* number of idle clients */
	uint64 tx_bytes;          /* amount of data sent to client */
	uint64 rx_bytes;          /* amount of data send to server */
	uint64 n_transactions;    /* total number of proroceeded transactions */
} ConnectionProxyState;

extern ConnectionProxyState* ProxyState;
extern PGDLLIMPORT int MyProxyId;
extern PGDLLIMPORT pgsocket MyProxySocket;

extern int  ConnectionProxyStart(void);
extern int  ConnectionProxyShmemSize(void);
extern void ConnectionProxyShmemInit(void);
#ifdef EXEC_BACKEND
extern void ConnectionProxyMain(int argc, char *argv[]);
#endif

#endif
