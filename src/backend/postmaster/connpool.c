/*-------------------------------------------------------------------------
 * connpool.c
 *	   PostgreSQL connection pool workers.
 *
 * Copyright (c) 2018, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/backend/postmaster/connpool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>

#include "lib/stringinfo.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/connpool.h"
#include "postmaster/postmaster.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "tcop/tcopprot.h"

/*
 * GUC parameters
 */
int			NumConnPoolWorkers = 2;

/*
 * Global variables
 */
ConnPoolWorker	*ConnPoolWorkers;

/*
 * Signals management
 */
static volatile sig_atomic_t shutdown_requested = false;
static void handle_sigterm(SIGNAL_ARGS);

static void *pqstate;

static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;
	shutdown_requested = true;
	SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

Size
ConnPoolShmemSize(void)
{
	return MAXALIGN(sizeof(ConnPoolWorker) * NumConnPoolWorkers);
}

void
ConnectionPoolWorkersInit(void)
{
	int		i;
	bool	found;
	Size	size = ConnPoolShmemSize();

	ConnPoolWorkers = ShmemInitStruct("connection pool workers",
			size, &found);

	if (!found)
	{
		MemSet(ConnPoolWorkers, 0, size);
		for (i = 0; i < NumConnPoolWorkers; i++)
		{
			ConnPoolWorker	*worker = &ConnPoolWorkers[i];
			if (socketpair(AF_UNIX, SOCK_STREAM, 0, worker->pipes) < 0)
				elog(FATAL, "could not create socket pair for connection pool");
		}
	}
}

/*
 * Register background workers for startup packet reading.
 */
void
RegisterConnPoolWorkers(void)
{
	int					i;
	BackgroundWorker	bgw;

	if (SessionPoolSize == 0)
		/* no need to start workers */
		return;

	for (i = 0; i < NumConnPoolWorkers; i++)
	{
		memset(&bgw, 0, sizeof(bgw));
		bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
		bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
		snprintf(bgw.bgw_library_name, BGW_MAXLEN, "postgres");
		snprintf(bgw.bgw_function_name, BGW_MAXLEN, "StartupPacketReaderMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN,
				 "connection pool worker %d", i + 1);
		bgw.bgw_restart_time = 3;
		bgw.bgw_notify_pid = 0;
		bgw.bgw_main_arg = (Datum) i;

		RegisterBackgroundWorker(&bgw);
	}

	elog(LOG, "Connection pool have been started");
}

static void
resetWorkerState(ConnPoolWorker *worker, Port *port)
{
	/* Cleanup */
	whereToSendOutput = DestNone;
	if (port != NULL)
	{
		if (port->sock != PGINVALID_SOCKET)
			closesocket(port->sock);
		if (port->pqcomm_waitset != NULL)
			FreeWaitEventSet(port->pqcomm_waitset);
		port = NULL;
	}
	pq_set_current_state(pqstate, NULL, NULL);
}

void
StartupPacketReaderMain(Datum arg)
{
	sigjmp_buf	local_sigjmp_buf;
	ConnPoolWorker *worker = &ConnPoolWorkers[(int) arg];
	MemoryContext	mcxt;
	int				status;
	Port		   *port = NULL;

	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	mcxt = AllocSetContextCreate(TopMemoryContext,
								 "temporary context",
							     ALLOCSET_DEFAULT_SIZES);
	pqstate = pq_init(TopMemoryContext);
	worker->pid = MyProcPid;
	worker->latch = MyLatch;
	Assert(MyLatch == &MyProc->procLatch);

	MemoryContextSwitchTo(mcxt);

	/* In an exception is encountered, processing resumes here */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log and to the client */
		EmitErrorReport();

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(mcxt);
		FlushErrorState();

		/*
		 * We only reset worker state here, but memory will be cleaned
		 * after next cycle. That's enough for now.
		 */
		resetWorkerState(worker, port);

		/* Ready for new sockets */
		worker->state = CPW_FREE;

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	while (!shutdown_requested)
	{
		ListCell	   *lc;
		int				rc;
		StringInfoData	buf;

		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_POSTMASTER_DEATH,
				0, PG_WAIT_EXTENSION);

		if (rc & WL_POSTMASTER_DEATH)
			break;

		ResetLatch(&MyProc->procLatch);

		if (shutdown_requested)
			break;

		if (worker->state != CPW_NEW_SOCKET)
			/* we woke up for other reason */
			continue;

		/* Set up temporary pq state for startup packet */
		port = palloc0(sizeof(Port));
		port->sock = PGINVALID_SOCKET;

		while (port->sock == PGINVALID_SOCKET)
			port->sock = pg_recv_sock(worker->pipes[1]);

		/* init pqcomm */
		port->pqcomm_waitset = pq_create_backend_event_set(mcxt, port, true);
		port->canAcceptConnections = worker->cac_state;
		pq_set_current_state(pqstate, port, port->pqcomm_waitset);
		whereToSendOutput = DestRemote;

		/* TODO: deal with timeouts */
		status = ProcessStartupPacket(port, false, mcxt, ERROR);
		if (status != STATUS_OK)
		{
			worker->state = CPW_FREE;
			goto cleanup;
		}

		/* Serialize a port into stringinfo */
		pq_beginmessage(&buf, 'P');
		pq_sendint(&buf, port->proto, 4);
		pq_sendstring(&buf, port->database_name);
		pq_sendstring(&buf, port->user_name);
		pq_sendint(&buf, list_length(port->guc_options), 4);

		foreach(lc, port->guc_options)
		{
			char *str = (char *) lfirst(lc);
			pq_sendstring(&buf, str);
		}

		if (port->cmdline_options)
		{
			pq_sendint(&buf, 1, 4);
			pq_sendstring(&buf, port->cmdline_options);
		}
		else pq_sendint(&buf, 0, 4);

		worker->state = CPW_PROCESSED;

		while ((rc = send(worker->pipes[1], &buf.len, sizeof(buf.len), 0)) < 0 && errno == EINTR);
		if (rc != (int)sizeof(buf.len))
			elog(ERROR, "could not send data to postmaster");
		while ((rc = send(worker->pipes[1], buf.data, buf.len, 0)) < 0 && errno == EINTR);
		if (rc != buf.len)
			elog(ERROR, "could not send data to postmaster");
		pfree(buf.data);
		buf.data = NULL;
	  cleanup:
		resetWorkerState(worker, port);
		MemoryContextReset(mcxt);
	}

	resetWorkerState(worker, NULL);
}
