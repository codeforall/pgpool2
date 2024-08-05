/*
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2024	PgPool Global Development Group
 *
 * Permission to use, copy, modify, and distribute this software and
 * its documentation for any purpose and without fee is hereby
 * granted, provided that the above copyright notice appear in all
 * copies and that both that copyright notice and this permission
 * notice appear in supporting documentation, and that the name of the
 * author not be used in advertising or publicity pertaining to
 * distribution of the software without specific, written prior
 * permission. The author makes no representations about the
 * suitability of this software for any purpose.  It is provided "as
 * is" without express or implied warranty.
 *
 */


#ifndef POOL_IPC_H
#define POOL_IPC_H

#include "pool.h"
typedef enum
{
    OPERATION_SUCCESS = 0,
    OPERATION_FAILED,
    CONNECTION_FAILED,
    SOCKETS_RECEIVED,
    DISCARD_AND_RECREATE,
    EMPTY_POOL_SLOT_RESERVED,
    NO_POOL_SLOT_AVAILABLE,
    NON_POOL_CONNECTION,
    INVALID_OPERATION,
    INVALID_RESPONSE
}	POOL_IPC_RETURN_CODES;

extern LEASE_TYPES BorrowBackendConnection(int parent_link, char* database,
                    char* user, int major, int minor, int *count, int* sockets);
extern bool ProcessChildRequestOnMain(IPC_Endpoint* ipc_endpoint);
extern bool TransferSocketsBetweenProcesses(int process_link, int count, int *sockets);
extern bool InformLeaseStatusToChild(int child_link, LEASE_TYPES lease_type);
extern bool ReleasePooledConnectionFromChild(int parent_link, bool discard);
extern bool SendBackendSocktesToMainPool(int parent_link, int count, int *sockets);
#endif /* POOL_IPC_H */
