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
#include "protocol/pool_connection_pool.h"
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


#define IPC_SOCKET_TRANSFER_MESSAGE      'S'
#define IPC_NO_CONN_AVAILABLE_MESSAGE    'F'
#define IPC_CONNECT_AND_PROCEED_MESSAGE  'E'
#define IPC_NON_POOL_CONNECT_MESSAGE     'N'

#define IPC_BORROW_CONNECTION_REQUEST    'B'
#define IPC_RELEASE_CONNECTION_REQUEST   'R'

#define MAX_WAIT_FOR_PARENT_RESPONSE 5 /* TODO should it be configurable ?*/

static bool receive_sockets(int fd, int count, int *sockets);
static bool process_borrow__connection_request(IPC_Endpoint* ipc_endpoint);
/*
 * To keep the packet small we use ProcessInfo for communicating the
 * required connection credentials */
POOL_IPC_RETURN_CODES
BorrowBackendConnection(int	parent_link, char* database, char* user, int major, int minor, int *count, int* sockets)
{
    char type = IPC_BORROW_CONNECTION_REQUEST;

	if (processType != PT_CHILD)
		return INVALID_OPERATION;

    /* write the information in procInfo*/
    ProcessInfo *pro_info = pool_get_my_process_info();
    StrNCpy(pro_info->database, database, SM_DATABASE);
    StrNCpy(pro_info->user, user, SM_USER);
    pro_info->major = major;
    pro_info->minor = minor;
    /* Send the message to main process */
    if (write(parent_link, &type, 1) != 1)
    {
		close(parent_link);
        ereport(FATAL,
                (errmsg("failed to write IPC packet type:%c to parent:%d", type, parent_link)));
        return CONNECTION_FAILED;
    }
    /* Since the child needs a socket to procees further, So wait for the reply */

    /*
     * Possible responses
     * 1. IPC_SOCKET_TRANSFER_MESSAGE:   Parent returns the array of sockets along with pool_index
     * 2. IPC_NO_CONN_AVAILABLE_MESSAGE: No free connection availble. POOL is fully occupied
     * 3. IPC_CONNECT_AND_PROCEED_MESSAGE: Parent asks the child to connect to the backend and proceed
     *                                    Child will return the connection to parent after use
     * 4. IPC_PACKET_NON_POOL_CONNECT:  The particular DB-USER pair is not eligible for pooling
     *
     */
	if (socket_read(parent_link, &type, 1, MAX_WAIT_FOR_PARENT_RESPONSE) != 1)
    {
		close(parent_link);
        ereport(FATAL,
                (errmsg("failed to read IPC packet type:%c from parent:%d", type, parent_link)));
        return CONNECTION_FAILED;
    }

    if (type == IPC_SOCKET_TRANSFER_MESSAGE)
    {
        BackendEndPoint* backend_end_point = GetBackendEndPoint(pro_info->pool_id);
        if (backend_end_point == NULL)
        {
            ereport(WARNING,
                    (errmsg("failed to get backend end point for pool_id:%d", pro_info->pool_id)));
            return OPERATION_FAILED;
        }
        /* ProcessInfo should already have the socket count */
        *count = backend_end_point->num_sockets;
        if(receive_sockets(parent_link, *count, sockets))
            return SOCKETS_RECEIVED;
        return OPERATION_FAILED;
    }
    else if (type == IPC_NO_CONN_AVAILABLE_MESSAGE)
        return NO_POOL_SLOT_AVAILABLE;
    else if (type == IPC_CONNECT_AND_PROCEED_MESSAGE)
        return EMPTY_POOL_SLOT_RESERVED;
    else if (type == IPC_NON_POOL_CONNECT_MESSAGE)
        return NON_POOL_CONNECTION;
    return INVALID_RESPONSE;
}

bool
ProcessChildRequestOnMain(IPC_Endpoint* ipc_endpoint)
{
    char type;
	if (processType != PT_MAIN)
		return false;

    if (socket_read(ipc_endpoint->child_link, &type, 1, MAX_WAIT_FOR_PARENT_RESPONSE) != 1)
    {
        ereport(LOG,
                (errmsg("failed to read IPC packet type:%c from child:%d", type, ipc_endpoint->child_pid)));
        return false;
    }
    if (type == IPC_BORROW_CONNECTION_REQUEST)
        return process_borrow__connection_request(ipc_endpoint);

    ereport(LOG,
            (errmsg("failed to process unsupported IPC packet type:%c from child:%d", type, ipc_endpoint->child_pid)));

    return false;
}

static bool
process_borrow__connection_request(IPC_Endpoint* ipc_endpoint)
{
    return LeasePooledConnectionToChild(ipc_endpoint);
}

bool
TransferSocketsBetweenProcesses(int process_link, int count, int *sockets)
{
    char type = IPC_SOCKET_TRANSFER_MESSAGE;
    if (write(process_link, &type, 1) != 1)
    {
        ereport(WARNING,
                (errmsg("failed to write IPC packet type:%c to parent", type)));
        return false;
    }
    if (ancil_send_fds(process_link, sockets, count) == -1)
    {
        ereport(WARNING,
                (errmsg("ancil_send_fds failed")));
        return false;
    }
    return true;
}

static bool
receive_sockets(int fd, int count, int *sockets)
{
    if (ancil_recv_fds(fd, sockets, count) == -1)
    {
        ereport(WARNING,
                (errmsg("ancil_recv_fds failed")));
        return false;
    }
    return true;
}

