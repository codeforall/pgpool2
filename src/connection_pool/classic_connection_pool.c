/* -*-classic_connection_pool-c-*- */
/*
 * $Header$
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
 */

#include "pool.h"
#include "connection_pool/connection_pool.h"
#include "connection_pool/backend_connection.h"
#include "context/pool_process_context.h"
#include "context/pool_query_context.h"
#include "protocol/pool_process_query.h"
#include "main/pgpool_ipc.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/pool_stream.h"
#include "pool_config.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

typedef struct LPBorrowConnectionRes
{
    LEASE_TYPES lease_type;
    int pool_id;
    ConnectionPoolEntry *selected_pool;
} LPBorrowConnectionRes;

extern ConnectionPoolEntry *ConnectionPool;
ConnectionPoolEntry *firstChildConnectionPool = NULL;

static bool load_pooled_connection_into_child(ConnectionPoolEntry *selected_pool, LEASE_TYPES lease_type);
static bool export_classic_cluster_connection_data_to_pool(void);
static bool copy_classic_cluster_sockets_pool(void);
static void import_pooled_startup_packet_into_child(PooledBackendClusterConnection *backend_end_point);
static ConnectionPoolEntry *get_pool_entry_for_pool_id(int pool_id);
static int terminate_idle_classic_connections(void);

static LEASE_TYPES pool_get_cp(char *database, char *user, int protoMajor, bool check_socket, ConnectionPoolEntry **selected_pool);
static ConnectionPoolEntry *get_pool_entry_to_discard(void);
static void discard_cp(void);
static void clear_pooled_cluster_connection(PooledBackendClusterConnection *backend_end_point);
static void close_all_pooled_connections(void);

static int classic_connection_pool_entry_count(void)
{
    return pool_config->max_pool * pool_config->num_init_children;
}
static const char *
LPGetConnectionPoolInfo(void)
{
    return "Classic Connection Pool";
}
static size_t
LPRequiredSharedMemSize(void)
{
    return sizeof(ConnectionPoolEntry) * classic_connection_pool_entry_count();
}

static int
LPGetPoolEntriesCount(void)
{
    return classic_connection_pool_entry_count();
}

static void
LPInitializeConnectionPool(void *shared_mem_ptr)
{
    int i;
    Assert(processType == PT_MAIN);
    ConnectionPool = (ConnectionPoolEntry *)shared_mem_ptr;
    memset(ConnectionPool, 0, LPRequiredSharedMemSize());

    for (i = 0; i < classic_connection_pool_entry_count(); i++)
    {
        ConnectionPool[i].pool_id = i;
        ConnectionPool[i].child_id = -1;
        ConnectionPool[i].status = POOL_ENTRY_EMPTY;
        ConnectionPool[i].child_pid = -1;
    }
}

/*
 * Either return existing connection or
 * Discard the victum connection and return
 * empty slot
 */
static BorrowConnectionRes *
LPBorrowClusterConnection(char *database, char *user, int major, int minor)
{
    LPBorrowConnectionRes *res = palloc(sizeof(LPBorrowConnectionRes));
    ConnectionPoolEntry *selected_pool = NULL;
    Assert(firstChildConnectionPool);
    res->lease_type = pool_get_cp(database, user, major, true, &selected_pool);
    /* set the pool_id*/
    res->pool_id = selected_pool ? selected_pool->pool_id : -1;
    res->selected_pool = selected_pool;
    if (selected_pool)
        ConnectionPoolRegisterNewLease(selected_pool, res->lease_type, my_proc_id, selected_pool->child_pid);

    return (BorrowConnectionRes*)res;
}

static void
LPLoadChildConnectionPool(int int_arg)
{
    int i;
    int child_id = my_proc_id;
    Assert(child_id < pool_config->num_init_children);
    firstChildConnectionPool = &ConnectionPool[child_id * pool_config->max_pool];
    elog(DEBUG2, "LoadChildConnectionPool: child_id:%d first id=%d", child_id, firstChildConnectionPool->pool_id);
    for (i = 0; i < pool_config->max_pool; i++)
    {
        firstChildConnectionPool[i].status = POOL_ENTRY_EMPTY;
        firstChildConnectionPool[i].child_id = child_id;
        firstChildConnectionPool[i].pool_id = i;
        firstChildConnectionPool[i].child_pid = getpid();
    }
}

static bool
LPLoadBorrowedConnection(BorrowConnectionRes *context)
{
    LPBorrowConnectionRes *lp_context = (LPBorrowConnectionRes *)context;
    return load_pooled_connection_into_child(lp_context->selected_pool, lp_context->lease_type);
}

static bool
LPSyncClusterConnectionDataInPool(void)
{
    return export_classic_cluster_connection_data_to_pool();
}

static bool
LPPushClusterConnectionToPool(void)
{
    return copy_classic_cluster_sockets_pool();
}

static bool
LPReleaseClusterConnection(bool discard)
{
    BackendClusterConnection *current_backend_con;
    ConnectionPoolEntry *pool_entry;

    Assert(processType == PT_CHILD);

    current_backend_con = GetBackendClusterConnection();
    pool_entry = get_pool_entry_for_pool_id(current_backend_con->pool_id);

    if (!pool_entry)
        return false;

    ConnectionPoolUnregisterLease(pool_entry, my_proc_id, pool_entry->child_pid);

    // if (pool_entry->child_pid != my_proc_id)
    // {
    //     ereport(WARNING,
    //             (errmsg("child:%d leased:%d is not the borrower of pool_id:%d borrowed by:%d",
    //                     ipc_endpoint->child_pid,
    //                     pro_info->pool_id,
    //                     pool_entry->pool_id,
    //                     pool_entry->child_pid)));
    //     return false;
    // }

    ereport(LOG,
            (errmsg("child: released pool_id:%d database:%s user:%s",
                    pool_entry->pool_id,
                    pool_entry->endPoint.database,
                    pool_entry->endPoint.user)));
    if (discard)
    {
        pool_entry->status = POOL_ENTRY_EMPTY;
        memset(&pool_entry->endPoint, 0, sizeof(PooledBackendClusterConnection));
    }

    return true;
}

static bool
LPClusterConnectionNeedPush(void)
{
    BackendClusterConnection *child_connection = GetBackendClusterConnection();
    ConnectionPoolEntry *pool_entry = get_pool_entry_for_pool_id(child_connection->pool_id);
    if (!pool_entry)
        return false;
    if (pool_entry->status == POOL_ENTRY_CONNECTED && child_connection->lease_type == LEASE_TYPE_READY_TO_USE)
        return false;
    return true;
}

static void
LPReleaseChildConnectionPool(void)
{
    close_all_pooled_connections();
}

static ConnectionPoolEntry *
ClassicConnectionPoolGetPoolEntry(int pool_id, int child_id)
{
    ConnectionPoolEntry *pool_entry;
    Assert(child_id < pool_config->num_init_children);
    Assert(pool_id < pool_config->max_pool);

    if (ConnectionPool == NULL)
        return NULL;
    pool_entry = &ConnectionPool[child_id * pool_config->max_pool];
    return &pool_entry[pool_id];
}

static void
LPUpdatePooledConnectionCount(void)
{
    int pool;
    int count = 0;
    ConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    if (!connection_pool)
        return;

    for (pool = 0; pool < pool_config->max_pool; pool++)
    {
        ConnectionPoolEntry *pool_entry = &connection_pool[pool];
        PooledBackendClusterConnection *endPoint = &pool_entry->endPoint;
        if (pool_entry->status != POOL_ENTRY_EMPTY)
            count++;
    }
    pool_get_my_process_info()->pooled_connections = count;
}

static int
LPDoHouseKeeping(void)
{
    if (processType != PT_CHILD)
        return 0;
    if (pool_config->connection_life_time > 0)
        return terminate_idle_classic_connections();
    return 0;
}

ConnectionPoolRoutine ClassicConnectionPoolRoutine = {
    .RequiredSharedMemSize = LPRequiredSharedMemSize,
    .InitializeConnectionPool = LPInitializeConnectionPool,
    .LoadChildConnectionPool = LPLoadChildConnectionPool,
    .BorrowClusterConnection = LPBorrowClusterConnection,
    .LoadBorrowedConnection = LPLoadBorrowedConnection,
    .ReleaseClusterConnection = LPReleaseClusterConnection,
    .SyncClusterConnectionDataInPool = LPSyncClusterConnectionDataInPool,
    .PushClusterConnectionToPool = LPPushClusterConnectionToPool,
    .ReleaseChildConnectionPool = LPReleaseChildConnectionPool,
    .ClusterConnectionNeedPush = LPClusterConnectionNeedPush,
    .GetConnectionPoolInfo = LPGetConnectionPoolInfo,
    .GetPoolEntriesCount = LPGetPoolEntriesCount,
    .GetConnectionPoolEntry = ClassicConnectionPoolGetPoolEntry,
    .UpdatePooledConnectionCount = LPUpdatePooledConnectionCount,
    .DoHouseKeeping = LPDoHouseKeeping
    };

const ConnectionPoolRoutine *
GetClassicConnectionPool(void)
{
    return &ClassicConnectionPoolRoutine;
}

static ConnectionPoolEntry *
get_pool_entry_for_pool_id(int pool_id)
{
    if (pool_id < 0 || pool_id >= pool_config->max_pool)
        return NULL;
    return &firstChildConnectionPool[pool_id];
}

/*
 * We should already have the sockets imported from the global pool
 */
static bool
load_pooled_connection_into_child(ConnectionPoolEntry *selected_pool, LEASE_TYPES lease_type)
{
    int i;
    PooledBackendClusterConnection *backend_end_point;
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    Assert(selected_pool);
    backend_end_point = &selected_pool->endPoint;

    ereport(DEBUG2,
            (errmsg("load_pooled_connection_into_child pool_id:%d backend_end_point:%p LeaseType:%d", selected_pool->pool_id, backend_end_point, lease_type)));

    current_backend_con->pool_id = selected_pool->pool_id;
    current_backend_con->backend_end_point = backend_end_point;
    current_backend_con->lease_type = lease_type;

    if (lease_type == LEASE_TYPE_EMPTY_SLOT_RESERVED)
        return true;

    import_pooled_startup_packet_into_child(backend_end_point);

    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (! VALID_BACKEND(i))
            continue;
        // current_backend_con->slots[i].socket = backend_end_point->conn_slots[i].socket;
        current_backend_con->slots[i].con = pool_open(backend_end_point->conn_slots[i].socket, true);
        current_backend_con->slots[i].con->pooled_backend_ref = &backend_end_point->conn_slots[i];

        current_backend_con->slots[i].key = backend_end_point->conn_slots[i].key;
        current_backend_con->slots[i].pid = backend_end_point->conn_slots[i].pid;
        current_backend_con->slots[i].state = CONNECTION_SLOT_LOADED_FROM_BACKEND;
    }

    return true;
}

static bool
copy_classic_cluster_sockets_pool(void)
{
    int i;
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();
    PooledBackendClusterConnection *backend_end_point = current_backend_con->backend_end_point;
    int pool_id = current_backend_con->pool_id;

    Assert(backend_end_point);

    for (i=0; i< NUM_BACKENDS; i++)
    {
        if (!VALID_BACKEND(i))
            continue;
        if (current_backend_con->slots[i].con == NULL)
            backend_end_point->conn_slots[i].socket = -1;
        else
            backend_end_point->conn_slots[i].socket = current_backend_con->slots[i].con->fd;
    }

    get_pool_entry_for_pool_id(pool_id)->status = POOL_ENTRY_CONNECTED;
    return true;
}

static bool
export_classic_cluster_connection_data_to_pool(void)
{
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();
    StartupPacket *sp = current_backend_con->sp;
    int pool_id = current_backend_con->pool_id;
    int i, sock_index;
    ConnectionPoolEntry *pool_entry = get_pool_entry_for_pool_id(pool_id);
    PooledBackendClusterConnection *backend_end_point;

    if (pool_entry == NULL)
        return false;
    backend_end_point = &pool_entry->endPoint;

    if (backend_end_point == NULL)
        return false;

    /* verify the length first */
    if (sp->len <= 0 || sp->len >= MAX_STARTUP_PACKET_LENGTH)
    {
        ereport(ERROR,
                (errmsg("incorrect startup packet length (%d)", sp->len)));
        return false;
    }

    current_backend_con->backend_end_point = backend_end_point;
    memcpy(backend_end_point->startup_packet_data, sp->startup_packet, sp->len);
    backend_end_point->sp.len = sp->len;
    backend_end_point->sp.startup_packet = backend_end_point->startup_packet_data;

    backend_end_point->sp.major = sp->major;
    backend_end_point->sp.minor = sp->minor;

    StrNCpy(backend_end_point->database, sp->database, sizeof(backend_end_point->database));
    StrNCpy(backend_end_point->user, sp->user, sizeof(backend_end_point->user));

    backend_end_point->sp.database = backend_end_point->database;
    backend_end_point->sp.user = backend_end_point->user;

    if (sp->major == PROTO_MAJOR_V3 && sp->application_name)
    {
        /* adjust the application name pointer in new packet */
        backend_end_point->sp.application_name = backend_end_point->sp.startup_packet + (sp->application_name - sp->startup_packet);
    }
    else
        backend_end_point->sp.application_name = NULL;

    sock_index = 0;
    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (VALID_BACKEND(i))
        {
            backend_end_point->conn_slots[i].key = current_backend_con->slots[i].key;
            backend_end_point->conn_slots[i].pid = current_backend_con->slots[i].pid;
            backend_end_point->backend_ids[sock_index++] = i;
        }
        else
        {
            backend_end_point->conn_slots[i].key = -1;
            backend_end_point->conn_slots[i].pid = -1;
            backend_end_point->conn_slots[i].socket = -1;
        }
    }
    backend_end_point->num_sockets = sock_index;
    get_pool_entry_for_pool_id(pool_id)->status = POOL_ENTRY_LOADED;
    return true;
}

static void
import_pooled_startup_packet_into_child(PooledBackendClusterConnection *backend_end_point)
{

    if (backend_end_point->sp.len <= 0 || backend_end_point->sp.len >= MAX_STARTUP_PACKET_LENGTH)
        ereport(ERROR,
                (errmsg("incorrect packet length (%d)", backend_end_point->sp.len)));

    ImportStartupPacketIntoChild(&backend_end_point->sp, backend_end_point->startup_packet_data);
}

/*
 * find connection by user and database
 */
static LEASE_TYPES
pool_get_cp(char *database, char *user, int protoMajor, bool check_socket, ConnectionPoolEntry **selected_pool)
{
    pool_sigset_t oldmask;
    int i;
    ConnectionPoolEntry *first_empty_entry = NULL;
    ConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    Assert(connection_pool);

    POOL_SETMASK2(&BlockSig, &oldmask);

    for (i = 0; i < pool_config->max_pool; i++)
    {
        PooledBackendClusterConnection *endPoint = &connection_pool[i].endPoint;
        *selected_pool = &connection_pool[i];
        if (connection_pool[i].status == POOL_ENTRY_EMPTY)
        {
            if (!first_empty_entry)
                first_empty_entry = &connection_pool[i];
            continue;
        }
        if (!strcmp(endPoint->user, user) &&
            !strcmp(endPoint->database, database) &&
            endPoint->sp.major == protoMajor)
            {
                int sock_broken = 0;
                int j;

                /* mark this connection is under use */
                endPoint->client_connected = true;
                POOL_SETMASK(&oldmask);

                if (check_socket)
                {
                    for (j = 0; j < NUM_BACKENDS; j++)
                    {
                        PooledBackendNodeConnection *pooled_backend_node = &endPoint->conn_slots[j];
                        if (!VALID_BACKEND(j))
                            continue;
                        if (pooled_backend_node->socket > 0)
                        {
                            sock_broken = check_socket_status(pooled_backend_node->socket);
                            if (sock_broken < 0)
                                break;
                        }
                        else
                        {
                            sock_broken = -1;
                            break;
                        }
                    }

                    if (sock_broken < 0)
                    {
                        ereport(LOG,
                                (errmsg("connection closed."),
                                 errdetail("retry to create new connection pool")));
                        /*
                         * It is possible that one of backend just broke.  sleep 1
                         * second to wait for failover occurres, then wait for the
                         * failover finishes.
                         */
                        sleep(1);
                        wait_for_failover_to_finish();

                        for (j = 0; j < NUM_BACKENDS; j++)
                        {
                            PooledBackendNodeConnection *pooled_backend_node = &endPoint->conn_slots[j];
                            if (!VALID_BACKEND(j) || pooled_backend_node->socket <= 0)
                                continue;
                            close(pooled_backend_node->socket);
                            pooled_backend_node->socket = -1;
                        }
                        clear_pooled_cluster_connection(endPoint);
                        POOL_SETMASK(&oldmask);

                        connection_pool[i].status = POOL_ENTRY_EMPTY;
                        return LEASE_TYPE_EMPTY_SLOT_RESERVED;
                    }
                }
                POOL_SETMASK(&oldmask);

                return LEASE_TYPE_READY_TO_USE;
            }
    }
    POOL_SETMASK(&oldmask);
    if (first_empty_entry)
    {
        *selected_pool = first_empty_entry;
        elog(DEBUG2, "pool_get_cp: empty slot reserved LEASE_TYPE_EMPTY_SLOT_RESERVED at id %d", first_empty_entry->pool_id);
        return LEASE_TYPE_EMPTY_SLOT_RESERVED;
    }

    *selected_pool = get_pool_entry_to_discard();
    elog(DEBUG2, "pool_get_cp: discard and create LEASE_TYPE_DISCART_AND_CREATE");

    return LEASE_TYPE_DISCART_AND_CREATE;
}

/*
 * create a connection pool by user and database
 */
static ConnectionPoolEntry *
get_pool_entry_to_discard(void)
{
    int i;
    time_t closetime = TMINTMAX;
    ConnectionPoolEntry *oldestp;
    ConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    Assert(connection_pool);

    /*
     * no empty connection slot was found. look for the oldest connection and
     * discard it.
     */
    oldestp = connection_pool;

    for (i = 0; i < pool_config->max_pool; i++)
    {
        if (connection_pool[i].last_returned_time < closetime)
        {
            closetime = connection_pool[i].last_returned_time;
            oldestp = &connection_pool[i];
        }
    }
    return oldestp;
}

/*
 * disconnect and release a connection to the database
 */
static void
discard_cp(void)
{
    int i;
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    Assert(processType == PT_CHILD);
    Assert(current_backend_con);

    pool_send_frontend_exits(current_backend_con);

    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (!VALID_BACKEND(i))
            continue;
        pool_close(current_backend_con->slots[i].con, true);
        current_backend_con->slots[i].state = CONNECTION_SLOT_EMPTY;
    }
}

static void
clear_pooled_cluster_connection(PooledBackendClusterConnection *backend_end_point)
{
    memset(&backend_end_point->conn_slots, 0, sizeof(PooledBackendNodeConnection) * MAX_NUM_BACKENDS);
    backend_end_point->num_sockets = 0;
}

/*
 * Disconnect to all backend connections.  this is called
 * in any case when child process exits, for example failover, child
 * life time expires or child max connections expires.
 */

static void
close_all_pooled_connections(void)
{
    int pool;
    int current_pool_id = -1;
    ConnectionPoolEntry *connection_pool = firstChildConnectionPool;
    BackendClusterConnection *current_backend_con;

    Assert(processType == PT_CHILD);
    if (!connection_pool)
        return;

    current_backend_con = GetBackendClusterConnection();
    current_pool_id = current_backend_con->pool_id;
    pool_send_frontend_exits(current_backend_con);

    for (pool = 0; pool < pool_config->max_pool; pool++)
    {
        int i;
        ConnectionPoolEntry *pool_entry = &connection_pool[pool];
        PooledBackendClusterConnection *endPoint = &pool_entry->endPoint;
        if (pool_entry->status == POOL_ENTRY_EMPTY)
            continue;
        if (current_pool_id != pool)
        {
            TerminatePooledBackendClusterConnection(endPoint);
            memset(&pool_entry->endPoint, 0, sizeof(PooledBackendClusterConnection));
            pool_entry->status = POOL_ENTRY_EMPTY;
        }
    }
    ResetBackendClusterConnection();
}

static int
terminate_idle_classic_connections(void)
{
    int pool;
    time_t nearest = TMINTMAX;
    time_t now;
    ConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    Assert(connection_pool);

    now = time(NULL);
    if (now == ((time_t)-1))
    {
        ereport(WARNING, (errmsg("Failed to get current time")));
        return -1;
    }

    ereport(DEBUG2,
            (errmsg("backend timer handler called at %ld", now)));

    POOL_SETMASK(&BlockSig);

    for (pool = 0; pool < pool_config->max_pool; pool++)
    {
        ConnectionPoolEntry *pool_entry = &connection_pool[pool];
        PooledBackendClusterConnection *endPoint = &pool_entry->endPoint;
        int sec;

        if (pool_entry->status == POOL_ENTRY_EMPTY)
            continue;

        sec = now - pool_entry->last_returned_time;
        if (sec >= pool_config->connection_life_time)
        {
            /* discard expired connection */
            ereport(DEBUG2,
                    (errmsg("classic connection pool house keeping discarding pooled connection:%d idle for %d ses", pool, sec),
                     errdetail("expired user \"%s\" database \"%s\"",
                               endPoint->user, endPoint->database)));
            TerminatePooledBackendClusterConnection(endPoint);
            memset(&pool_entry->endPoint, 0, sizeof(PooledBackendClusterConnection));
            pool_entry->status = POOL_ENTRY_EMPTY;
            pool_entry->last_returned_time = TMINTMAX;
        }
        else
        {
            if (pool_entry->last_returned_time < nearest)
                nearest = pool_entry->last_returned_time;
        }
    }
    POOL_SETMASK(&UnBlockSig);
    if (nearest != TMINTMAX)
    {
        nearest = pool_config->connection_life_time - (now - nearest);
        if (nearest <= 0)
            nearest = 1;
        ereport(LOG,
                (errmsg("returning classic connection pool house keeping next check in %ld sec", nearest)));
        return nearest;
    }
    UpdatePooledConnectionCount();
    return 0;
}
