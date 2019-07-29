/*-------------------------------------------------------------------------
 *
 * libpqconn.c
 *
 * This file provides a way to establish connection to postgres instanc from backend.
 *
 * Portions Copyright (c) 2010-2018, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/libpqconn/libpqconn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/time.h>

#include "fmgr.h"
#include "libpq-fe.h"
#include "postmaster/postmaster.h"

PG_MODULE_MAGIC;

void _PG_init(void);

static void*
libpq_connectdb(char const* keywords[], char const* values[], char** error)
{
	PGconn* conn = PQconnectdbParams(keywords, values, false);
	if (conn && PQstatus(conn) != CONNECTION_OK)
	{
		ereport(WARNING,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not setup local connect to server"),
				 errdetail_internal("%s", pchomp(PQerrorMessage(conn)))));
		*error = strdup(PQerrorMessage(conn));
		PQfinish(conn);
		return NULL;
	}
	*error = NULL;
	return conn;
}

void _PG_init(void)
{
	LibpqConnectdbParams = libpq_connectdb;
}
