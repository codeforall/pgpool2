/* -*-pgsql-c-*- */
/*
 * $Header$
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2023	PgPool Global Development Group
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
 */

/*https://www.normalesup.org/~george/comp/libancillary/ */

/* Transfering and returning the connections should be kept as fast
 * as possible since this directly dictates the speed of client connection
 */

#include "pool.h"
#include "pool_config.h"
#include "main/pgpool_ipc.h"
#include "pool_config_variables.h"
#include "context/pool_process_context.h"
#include "connection_pool/connection_pool.h"
#include "utils/socket_stream.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/ancillary/ancillary.h"


#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>


#define IPC_READY_TO_USE_CONNECTION_MESSAGE     'S' /* Followd by sockets transfer */
#define IPC_DISCARD_AND_REUSE_MESSAGE           'D' /* Followd by sockets transfer */
#define IPC_NO_CONN_AVAILABLE_MESSAGE    'F'
#define IPC_CONNECT_AND_PROCEED_MESSAGE  'E'
#define IPC_NON_POOL_CONNECT_MESSAGE     'N'

#define IPC_BORROW_CONNECTION_REQUEST    'B'
#define IPC_RELEASE_CONNECTION_REQUEST   'R'
#define IPC_DISCARD_CONNECTION_REQUEST   'D'
#define IPC_PUSH_CONNECTION_TO_POOL      'P'

#define MAX_WAIT_FOR_PARENT_RESPONSE 5 /* TODO should it be configurable ?*/

static bool receive_sockets(int fd, int count, int *sockets);
static bool process_borrow__connection_request(IPC_Endpoint* ipc_endpoint);

static bool process_release_connection_request(IPC_Endpoint* ipc_endpoint);
static bool process_discard_connection_request(IPC_Endpoint* ipc_endpoint);

static bool  process_push_connection_to_pool(IPC_Endpoint* ipc_endpoint);

/*
 * To keep the packet small we use ProcessInfo for communicating the
 * required connection credentials */
LEASE_TYPES
BorrowBackendConnection(int	parent_link, char* database, char* user, int major, int minor, int *count, int* sockets)
{
    char type = IPC_BORROW_CONNECTION_REQUEST;

	if (processType != PT_CHILD)
		return LEASE_TYPE_INVALID;

    /* write the information in procInfo*/
    ProcessInfo *pro_info = pool_get_my_process_info();
    StrNCpy(pro_info->database, database, SM_DATABASE);
    StrNCpy(pro_info->user, user, SM_USER);
    pro_info->major = major;
    pro_info->minor = minor;
    /* Send the message to main process */

    ereport(DEBUG2,
            (errmsg("Asking pooled connection for:%s :%s ", pro_info->database, pro_info->user)));

    if (write(parent_link, &type, 1) != 1)
    {
		close(parent_link);
        ereport(FATAL,
                (errmsg("failed to write IPC packet type:%c to parent:%d", type, parent_link)));
        return LEASE_TYPE_LEASE_FAILED;
    }

    /*
     * Read the lease type response from main process
     */
	if (socket_read(parent_link, &type, 1, MAX_WAIT_FOR_PARENT_RESPONSE) != 1)
    {
		close(parent_link);
        ereport(FATAL,
                (errmsg("failed to read IPC packet type:%c from parent:%d", type, parent_link)));
        return LEASE_TYPE_LEASE_FAILED;
    }
    /*
     * In case of a successful lease. main process should already have updated the pool_id
     * in process info for this child
     */
    if (type == IPC_READY_TO_USE_CONNECTION_MESSAGE || type == IPC_DISCARD_AND_REUSE_MESSAGE)
    {
        PooledBackendClusterConnection *backend_end_point = GetGlobalPooledBackendClusterConnection(pro_info->pool_id);
        if (backend_end_point == NULL)
        {
            ereport(WARNING,
                    (errmsg("failed to get backend end point for pool_id:%d", pro_info->pool_id)));
            return LEASE_TYPE_LEASE_FAILED;
        }
        /* ProcessInfo should already have the socket count */
        *count = backend_end_point->num_sockets;
        if(receive_sockets(parent_link, *count, sockets))
        {
            if (type == IPC_READY_TO_USE_CONNECTION_MESSAGE)
                return LEASE_TYPE_READY_TO_USE;
            else if (type == IPC_DISCARD_AND_REUSE_MESSAGE)
                return LEASE_TYPE_DISCART_AND_CREATE;
        }
        return LEASE_TYPE_LEASE_FAILED;
    }
    else if (type == IPC_NO_CONN_AVAILABLE_MESSAGE)
        return LEASE_TYPE_NO_AVAILABLE_SLOT;
    else if (type == IPC_CONNECT_AND_PROCEED_MESSAGE)
        return LEASE_TYPE_EMPTY_SLOT_RESERVED;
    return LEASE_TYPE_INVALID;
}

bool
ProcessChildRequestOnMain(IPC_Endpoint* ipc_endpoint)
{
    char type;
	if (processType != PT_MAIN)
		return false;

    ereport(DEBUG2,
            (errmsg("New request received from from child:%d", ipc_endpoint->child_pid)));

    if (socket_read(ipc_endpoint->child_link, &type, 1, MAX_WAIT_FOR_PARENT_RESPONSE) != 1)
    {
        ereport(LOG,
                (errmsg("failed to read IPC packet type:%c from child:%d", type, ipc_endpoint->child_pid)));
        return false;
    }
    ereport(DEBUG2,
            (errmsg("Processing request type:%c from child:%d", type, ipc_endpoint->child_pid)));
    if (type == IPC_BORROW_CONNECTION_REQUEST)
        return process_borrow__connection_request(ipc_endpoint);
    else if (type == IPC_RELEASE_CONNECTION_REQUEST)
        return process_release_connection_request(ipc_endpoint);
    else if (type == IPC_DISCARD_CONNECTION_REQUEST)
        return process_discard_connection_request(ipc_endpoint);
    else if (type == IPC_PUSH_CONNECTION_TO_POOL)
        return process_push_connection_to_pool(ipc_endpoint);
    else

    ereport(LOG,
            (errmsg("failed to process unsupported IPC packet type:%c from child:%d", type, ipc_endpoint->child_pid)));

    return false;
}

bool
InformLeaseStatusToChild(int child_link, LEASE_TYPES lease_type)
{
    char type;
    switch (lease_type)
    {
	case LEASE_TYPE_READY_TO_USE:
        type = IPC_READY_TO_USE_CONNECTION_MESSAGE;
        break;
	case LEASE_TYPE_DISCART_AND_CREATE:
        type = IPC_DISCARD_AND_REUSE_MESSAGE;
        break;
	case LEASE_TYPE_EMPTY_SLOT_RESERVED:
        type = IPC_CONNECT_AND_PROCEED_MESSAGE;
        break;
	case LEASE_TYPE_NO_AVAILABLE_SLOT:
        type = IPC_NO_CONN_AVAILABLE_MESSAGE;
        break;
	case LEASE_TYPE_NON_POOL_CONNECTION:
        type = IPC_NON_POOL_CONNECT_MESSAGE;
        break;
    case LEASE_TYPE_INVALID:
    default:
        ereport(WARNING,
                (errmsg("unsupported lease_type:%d", lease_type)));
        return false;
        break;
    }

    if (write(child_link, &type, 1) != 1)
    {
        ereport(WARNING,
                (errmsg("failed to write IPC packet type:%c to child", type)));
        return false;
    }
    return true;
}

bool
SendBackendSocktesToMainPool(int parent_link, int count, int *sockets)
{
    char type = IPC_PUSH_CONNECTION_TO_POOL;
    if (write(parent_link, &type, 1) != 1)
    {
        ereport(WARNING,
                (errmsg("failed to write IPC packet type:%c to global pool", type)));
        return false;
    }
    return TransferSocketsBetweenProcesses(parent_link, count, sockets);
}

bool
ReleasePooledConnectionFromChild(int parent_link, bool discard)
{
    char type = discard?IPC_DISCARD_CONNECTION_REQUEST : IPC_RELEASE_CONNECTION_REQUEST;

    if (processType != PT_CHILD)
        return false;

    if (write(parent_link, &type, 1) != 1)
    {
        ereport(WARNING,
                (errmsg("failed to write IPC packet type:%c to global pool", type)));
        return false;
    }
    return true;
}

bool
TransferSocketsBetweenProcesses(int process_link, int count, int *sockets)
{
    if (ancil_send_fds(process_link, sockets, count) == -1)
    {
        ereport(WARNING,
                (errmsg("ancil_send_fds failed")));
        return false;
    }
    return true;
}

static bool
process_borrow__connection_request(IPC_Endpoint* ipc_endpoint)
{
    return GlobalPoolLeasePooledConnectionToChild(ipc_endpoint);
}

static bool
receive_sockets(int fd, int count, int *sockets)
{
    if (ancil_recv_fds(fd, sockets, count) == -1)
    {
        ereport(WARNING,
                (errmsg("ancil_recv_fds failed"),
                errdetail("%m")));
        return false;
    }
    return true;
}

static bool
process_discard_connection_request(IPC_Endpoint* ipc_endpoint)
{
    ProcessInfo *pro_info = NULL;
    ConnectionPoolEntry* pool_entry;

    if (processType != PT_MAIN)
        return false;

    ereport(DEBUG2,
        (errmsg("Processing Discard connection to pool from child:%d", ipc_endpoint->child_pid)));

    pro_info = pool_get_process_info_from_IPC_Endpoint(ipc_endpoint);
    if (!pro_info)
    {
        ereport(WARNING,
                (errmsg("failed to get process info for child:%d", ipc_endpoint->child_pid)));
        return false;
    }
    pool_entry = GetGlobalConnectionPoolEntry(pro_info->pool_id);
    if (!pool_entry)
    {
        ereport(WARNING,
                (errmsg("failed to get pool entry for pool_id:%d", pro_info->pool_id)));
        return false;
    }
    return GlobalPoolReleasePooledConnection(pool_entry, ipc_endpoint, false, true);
}

static bool
process_release_connection_request(IPC_Endpoint* ipc_endpoint)
{
    ProcessInfo *pro_info = NULL;
    ConnectionPoolEntry* pool_entry;

    if (processType != PT_MAIN)
        return false;

    ereport(LOG,
        (errmsg("Processing Release connection to pool from child:%d", ipc_endpoint->child_pid)));

    pro_info = pool_get_process_info_from_IPC_Endpoint(ipc_endpoint);
    if (!pro_info)
    {
        ereport(WARNING,
                (errmsg("failed to get process info for child:%d", ipc_endpoint->child_pid)));
        return false;
    }
    pool_entry = GetGlobalConnectionPoolEntry(pro_info->pool_id);
    if (!pool_entry)
    {
        ereport(WARNING,
                (errmsg("failed to get pool entry for pool_id:%d", pro_info->pool_id)));
        return false;
    }
    return GlobalPoolReleasePooledConnection(pool_entry, ipc_endpoint, false, false);
}

static bool
process_push_connection_to_pool(IPC_Endpoint* ipc_endpoint)
{
    ProcessInfo *pro_info = NULL;
    ConnectionPoolEntry* pool_entry;
    int		sockets[MAX_NUM_BACKENDS];

    if (processType != PT_MAIN)
        return false;

    ereport(LOG,
            (errmsg("Processing push connection to pool from child:%d", ipc_endpoint->child_pid)));

    pro_info = pool_get_process_info_from_IPC_Endpoint(ipc_endpoint);
    if (!pro_info)
    {
        ereport(WARNING,
                (errmsg("failed to get process info for child:%d", ipc_endpoint->child_pid)));
        return false;
    }
    pool_entry = GetGlobalConnectionPoolEntry(pro_info->pool_id);
    if (!pool_entry)
    {
        ereport(WARNING,
                (errmsg("failed to get pool entry for pool_id:%d", pro_info->pool_id)));
        return false;
    }

    ereport(DEBUG2,
            (errmsg("Expecting to receive %d socktes for pool_id:%d",pool_entry->endPoint.num_sockets, pro_info->pool_id)));

    if (receive_sockets(ipc_endpoint->child_link, pool_entry->endPoint.num_sockets, sockets) == true)
    {
        bool ret ;
        ereport(LOG,
                (errmsg("received %d sockets for pool_id:%d from child:%d", pool_entry->endPoint.num_sockets, pro_info->pool_id,ipc_endpoint->child_pid)));
        ret = InstallSocketsInConnectionPool(pool_entry, sockets);
        if (ret)
        {
            pool_entry->status = POOL_ENTRY_CONNECTED;
        }
        else
        {
            /* Mark this entry as empty, as we can't do anything witout socktes */
            pool_entry->status = POOL_ENTRY_EMPTY;
            ereport(LOG,
                (errmsg("InstallSocketsInConnectionPool failed for pool_id:%d from child:%d failed", pro_info->pool_id,ipc_endpoint->child_pid)));
            ret = GlobalPoolReleasePooledConnection(pool_entry, ipc_endpoint, !ret, false);
            if (!ret)
                ereport(LOG,
                        (errmsg("GlobalPoolReleasePooledConnection failed for pool_id:%d from child:%d failed", pro_info->pool_id, ipc_endpoint->child_pid)));
        }
        return ret;
    }
    else
    {
        ereport(WARNING,
                (errmsg("failed to receive %d sockets for pool_id:%d from child:%d", pool_entry->endPoint.num_sockets, pro_info->pool_id,ipc_endpoint->child_pid)));
        GlobalPoolReleasePooledConnection(pool_entry, ipc_endpoint, true, true);
    }

    return false;
}
