#ifndef CONN_POOL_H
#define CONN_POOL_H

#include "port.h"
#include "libpq/libpq-be.h"

#define MAX_CONNPOOL_WORKERS	100

typedef enum
{
	CPW_FREE,
	CPW_NEW_SOCKET,
	CPW_PROCESSED
} ConnPoolWorkerState;

enum CAC_STATE;

typedef struct ConnPoolWorker
{
	Port	   *port;		/* port in the pool */
	int			pipes[2];	/* 0 for sending, 1 for receiving */

	/* the communication procedure:
	 * ) find a worker with state == CPW_FREE
	 * ) assign client socket
	 * ) add pipe to wait set (if it's not there)
	 * ) wake up the worker.
	 * ) process data from the worker until state != CPW_PROCESSED
	 * ) set state to CPW_FREE
	 * ) fork or send socket and the data to backend.
	 *
	 * bgworker
	 * ) wokes up
	 * ) check the state
	 * ) if stats is CPW_NEW_SOCKET gets data from clientsock and
	 * send the data through pipe to postmaster.
	 * ) set state to CPW_PROCESSED.
	 */
	volatile ConnPoolWorkerState	state;
	volatile CAC_state				cac_state;
	pid_t							pid;
	Latch						   *latch;
} ConnPoolWorker;

extern Size ConnPoolShmemSize(void);
extern void ConnectionPoolWorkersInit(void);
extern void RegisterConnPoolWorkers(void);
extern void StartupPacketReaderMain(Datum arg);

/* global variables */
extern int NumConnPoolWorkers;
extern ConnPoolWorker *ConnPoolWorkers;

#endif
