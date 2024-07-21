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

#define TMINTMAX 0x7fffffff

typedef struct ClassicConnectionPoolEntry
{
    PooledBackendClusterConnection endPoint;
    POOL_ENTRY_STATUS status;
    int pool_id;
    int child_id;
    int classic_pool_id;
    pid_t borrower_pid;
    int borrower_proc_info_id;
    bool need_cleanup;
    time_t leased_time;
    time_t close_time;
    int leased_count;
} ClassicConnectionPoolEntry;

typedef struct LPBorrowConnectionRes
{
    LEASE_TYPES lease_type;
    int pool_id;
    ClassicConnectionPoolEntry* selected_pool;
} LPBorrowConnectionRes;

ClassicConnectionPoolEntry *ClassicConnectionPool = NULL; /* Classic connection pool still resides in shared memory */
ClassicConnectionPoolEntry *firstChildConnectionPool = NULL;

static bool load_pooled_connection_into_child(ClassicConnectionPoolEntry *selected_pool, LEASE_TYPES lease_type);
static bool export_classic_cluster_connection_data_to_pool(void);
static bool copy_classic_cluster_sockets_pool(void);
static void import_pooled_startup_packet_into_child(PooledBackendClusterConnection *backend_end_point);
static ClassicConnectionPoolEntry *get_pool_entry_for_pool_id(int pool_id);

static LEASE_TYPES pool_get_cp(char *user, char *database, int protoMajor, bool check_socket, ClassicConnectionPoolEntry *selected_pool);
static ClassicConnectionPoolEntry *get_pool_entry_to_discard(void);
static void discard_cp(void);
static void clear_pooled_cluster_connection(PooledBackendClusterConnection *backend_end_point);

static int classic_connection_pool_entry_count(void)
{
    return pool_config->max_pool_size * pool_config->num_init_children;
}

static size_t
LPRequiredSharedMemSize(void)
{
    return sizeof(ClassicConnectionPoolEntry) * classic_connection_pool_entry_count();
}

static void
LPInitializeConnectionPool(void *shared_mem_ptr)
{
    int i;
    ClassicConnectionPool = (ClassicConnectionPoolEntry *)shared_mem_ptr;
    memset(ClassicConnectionPool, 0, LPRequiredSharedMemSize());

    for (i = 0; i < classic_connection_pool_entry_count(); i++)
    {
        ClassicConnectionPool[i].pool_id = i;
        ClassicConnectionPool[i].borrower_proc_info_id = -1;
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
    ClassicConnectionPoolEntry *selected_pool = NULL;
    Assert(firstChildConnectionPool);
    res->lease_type = pool_get_cp(database, user, major, true, selected_pool);
    /* set the pool_id*/
    res->pool_id = selected_pool ? selected_pool->pool_id: -1;
    res->selected_pool = selected_pool;
    return (BorrowConnectionRes*)res;
}

static void
LPLoadChildConnectionPool(int child_id)
{
    int i;
    Assert(child_id < pool_config->num_init_children);
    firstChildConnectionPool = &ClassicConnectionPool[child_id * pool_config->max_pool_size];
    for (i = 0; i < pool_config->max_pool_size; i++)
    {
        firstChildConnectionPool[i].status = POOL_ENTRY_EMPTY;
        firstChildConnectionPool[i].child_id = child_id;
        firstChildConnectionPool[i].classic_pool_id = i;
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
    return true;
    return copy_classic_cluster_sockets_pool();
}

static bool
LPReleaseClusterConnection(bool discard)
{
    if (discard)
        discard_cp();
    return true;
}

/*
 * locate and return the shared memory BackendConnection having the
 * backend connection with the pid
 * If the connection is found the *backend_node_id contains the backend node id
 * of the backend node that has the connection
 */
static PooledBackendNodeConnection *
LPGetBackendNodeConnectionForBackendPID(int backend_pid, int *backend_node_id)
{
    int i;
    for (i = 0; i < classic_connection_pool_entry_count(); i++)
    {
        int con_slot;
        if (ClassicConnectionPool[i].status == POOL_ENTRY_EMPTY ||
            ClassicConnectionPool[i].endPoint.num_sockets <= 0)
            continue;
        for (con_slot = 0; con_slot < ClassicConnectionPool[i].endPoint.num_sockets; con_slot++)
        {
            if (ClassicConnectionPool[i].endPoint.conn_slots[con_slot].pid == backend_pid)
            {
                *backend_node_id = i;
                return &ClassicConnectionPool[i].endPoint.conn_slots[con_slot];
            }
        }
    }
    return NULL;
}

static bool
LPClusterConnectionNeedPush(void)
{
    BackendClusterConnection *child_connection = GetBackendClusterConnection();
    ClassicConnectionPoolEntry *pool_entry = get_pool_entry_for_pool_id(child_connection->pool_id);
    if (!pool_entry)
        return false;
    if (pool_entry->status == POOL_ENTRY_CONNECTED && child_connection->lease_type == LEASE_TYPE_READY_TO_USE)
        return false;
    return true;
}

static PooledBackendClusterConnection *
LPGetBackendEndPointForCancelPacket(CancelPacket *cp)
{
    int i;
    for (i = 0; i < classic_connection_pool_entry_count(); i++)
    {
        int con_slot;
        if (ClassicConnectionPool[i].status == POOL_ENTRY_EMPTY ||
            ClassicConnectionPool[i].endPoint.num_sockets <= 0)
            continue;

        for (con_slot = 0; con_slot < ClassicConnectionPool[i].endPoint.num_sockets; con_slot++)
        {
            PooledBackendNodeConnection *c = &ClassicConnectionPool[i].endPoint.conn_slots[con_slot];
            ereport(DEBUG2,
                    (errmsg("processing cancel request"),
                     errdetail("connection info: database:%s user:%s pid:%d key:%d i:%d",
                               ClassicConnectionPool[i].endPoint.database, ClassicConnectionPool[i].endPoint.user,
                               ntohl(c->pid), ntohl(c->key), con_slot)));
            if (c->pid == cp->pid && c->key == cp->key)
            {
                ereport(DEBUG1,
                        (errmsg("processing cancel request"),
                         errdetail("found pid:%d key:%d i:%d", ntohl(c->pid), ntohl(c->key), con_slot)));
                return &ClassicConnectionPool[i].endPoint;
            }
        }
    }
    return NULL;
}

static void
LPReleaseChildConnectionPool(void)
{
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
    .GetBackendNodeConnectionForBackendPID = LPGetBackendNodeConnectionForBackendPID,
    .ReleaseChildConnectionPool = LPReleaseChildConnectionPool,
    .ClusterConnectionNeedPush = LPClusterConnectionNeedPush,
    .GetBackendEndPointForCancelPacket = LPGetBackendEndPointForCancelPacket};

const ConnectionPoolRoutine *
GetClassicConnectionPool(void)
{
    return &ClassicConnectionPoolRoutine;
}

static ClassicConnectionPoolEntry *
get_pool_entry_for_pool_id(int pool_id)
{
    if (pool_id < 0 || pool_id >= pool_config->max_pool_size)
        return NULL;
    return &ClassicConnectionPool[pool_id];
}

/*
 * We should already have the sockets imported from the global pool
 */
static bool
load_pooled_connection_into_child(ClassicConnectionPoolEntry* selected_pool, LEASE_TYPES lease_type)
{
    int i;
    int *backend_ids;
    PooledBackendClusterConnection *backend_end_point;
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    Assert(selected_pool);

    backend_end_point = &selected_pool->endPoint;

    ereport(DEBUG2,
            (errmsg("load_pooled_connection_into_child pool_id:%d backend_end_point:%p LeaseType:%d", selected_pool->pool_id, backend_end_point, lease_type)));

    backend_ids = backend_end_point->backend_ids;

    current_backend_con->pool_id = selected_pool->pool_id;
    current_backend_con->backend_end_point = backend_end_point;
    current_backend_con->borrowed = true;
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
    return true;
}

static bool
export_classic_cluster_connection_data_to_pool(void)
{
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();
    StartupPacket *sp = current_backend_con->sp;
    int pool_id = current_backend_con->pool_id;
    int i, sock_index;
    PooledBackendClusterConnection *backend_end_point = GetGlobalPooledBackendClusterConnection(pool_id);

    if (backend_end_point == NULL)
        return false;

    /* verify the length first */
    if (sp->len <= 0 || sp->len >= MAX_STARTUP_PACKET_LENGTH)
    {
        ereport(ERROR,
                (errmsg("incorrect packet length (%d)", sp->len)));
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
pool_get_cp(char *user, char *database, int protoMajor, bool check_socket, ClassicConnectionPoolEntry *selected_pool)
{
    pool_sigset_t oldmask;
    int i;
    ClassicConnectionPoolEntry *first_empty_entry = NULL;
    ClassicConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    Assert(connection_pool)

    POOL_SETMASK2(&BlockSig, &oldmask);

    for (i = 0; i < pool_config->max_pool; i++)
    {
        PooledBackendClusterConnection *endPoint = &connection_pool[i].endPoint;
        selected_pool = &connection_pool[i];
        if (connection_pool[i].status == POOL_ENTRY_EMPTY)
        {
            if (!first_empty_entry)
                first_empty_entry = selected_pool;
            continue;
        }
        if (strcmp(endPoint->user, user) == 0 &&
            strcmp(endPoint->database, database) == 0 &&
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
        selected_pool = first_empty_entry;
        return LEASE_TYPE_EMPTY_SLOT_RESERVED;
    }

    selected_pool = get_pool_entry_to_discard();
    return LEASE_TYPE_DISCART_AND_CREATE;
}

/*
 * create a connection pool by user and database
 */
static ClassicConnectionPoolEntry *
get_pool_entry_to_discard(void)
{
    int i;
    time_t closetime;
    ClassicConnectionPoolEntry *oldestp;
    ClassicConnectionPoolEntry *connection_pool = firstChildConnectionPool;

    Assert(processType == PT_CHILD);
    Assert(connection_pool)

    /*
     * no empty connection slot was found. look for the oldest connection and
     * discard it.
     */
    oldestp = connection_pool;
    closetime = TMINTMAX;

    for (i = 0; i < pool_config->max_pool; i++)
    {

        // ereport(DEBUG1,
        //         (errmsg("creating connection pool"),
        //          errdetail("user: %s database: %s closetime: %ld",
        //                    CONNECTION_SLOT(p, main_node_id)->sp->user,
        //                    CONNECTION_SLOT(p, main_node_id)->sp->database,
        //                    CONNECTION_SLOT(p, main_node_id)->closetime)));

        if (connection_pool[i].close_time < closetime)
        {
            closetime = connection_pool[i].close_time;
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