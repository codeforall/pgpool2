#include "pool.h"
#include "connection_pool/connection_pool.h"
#include "connection_pool/backend_connection.h"
#include "context/pool_process_context.h"
#include "main/pgpool_ipc.h"
#include "utils/elog.h"
#include "utils/palloc.h"
#include "utils/pool_stream.h"
#include "pool_config.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

typedef struct GPBorrowConnectionRes
{
    LEASE_TYPES lease_type;
    int pool_id;
    int sockets[MAX_NUM_BACKENDS];
    int sock_count;
} GPBorrowConnectionRes;

extern ConnectionPoolEntry *ConnectionPool;

int ParentLinkFd = -1;

static bool import_pooled_connection_into_child(int pool_id, int *sockets, LEASE_TYPES lease_type);
static bool export_local_cluster_connection_data_to_pool(void);
static bool export_local_cluster_sockets_pool(void);
static void import_pooled_startup_packet_into_child(PooledBackendClusterConnection *backend_end_point);
static ConnectionPoolEntry* get_pool_entry_for_pool_id(int pool_id);
static int get_sockets_array(PooledBackendClusterConnection *backend_endpoint, int **sockets, int *num_sockets, bool pooled_socks);

static int get_pooled_connection_for_lending(char *user, char *database, int protoMajor, LEASE_TYPES *lease_type);
static bool register_new_lease(int pool_id, LEASE_TYPES lease_type, IPC_Endpoint *ipc_endpoint);
static bool unregister_lease(int pool_id, IPC_Endpoint *ipc_endpoint);

static int
GPGetPoolEntriesCount(void)
{
    return pool_config->max_pool_size;
}

static size_t
GPRequiredSharedMemSize(void)
{
    return sizeof(ConnectionPoolEntry) * GPGetPoolEntriesCount();
}

static const char*
GPGetConnectionPoolInfo(void)
{
    return "Global Connection Pool";
}

static void
GPInitializeConnectionPool(void *shared_mem_ptr)
{
    int i;
    ConnectionPool = (ConnectionPoolEntry *)shared_mem_ptr;
    memset(ConnectionPool, 0, GPRequiredSharedMemSize());

    for (i = 0; i < GPGetPoolEntriesCount(); i++)
    {
        ConnectionPool[i].pool_id = i;
        ConnectionPool[i].child_id = -1;
    }
}

static BorrowConnectionRes*
GPBorrowClusterConnection(char *database, char *user, int major, int minor)
{
    GPBorrowConnectionRes* res = palloc(sizeof(GPBorrowConnectionRes));
    ProcessInfo *pro_info = pool_get_my_process_info();
    
    Assert(ParentLinkFd != -1);
    res->lease_type = BorrowBackendConnection(ParentLinkFd, database, user, major, minor, &res->sock_count, res->sockets);
    /* set the pool_id*/
    res->pool_id = pro_info->pool_id;
    return (BorrowConnectionRes*)res;
}

static void
GPLoadChildConnectionPool(int intarg)
{
    ParentLinkFd = intarg;
}

static bool
GPLoadBorrowedConnection(BorrowConnectionRes *context)
{
    GPBorrowConnectionRes* gp_context = (GPBorrowConnectionRes*)context;
    return import_pooled_connection_into_child(gp_context->pool_id, gp_context->sockets, gp_context->lease_type);
}

static bool
GPSyncClusterConnectionDataInPool(void)
{
    return export_local_cluster_connection_data_to_pool();
}

static bool
GPPushClusterConnectionToPool(void)
{
    return export_local_cluster_sockets_pool();
}

static bool
GPReleaseClusterConnection(bool discard)
{
    return ReleasePooledConnectionFromChild(ParentLinkFd, discard);
}

static bool
GPClusterConnectionNeedPush(void)
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
GPReleaseChildConnectionPool(void)
{
}

ConnectionPoolRoutine GlobalConnectionPoolRoutine = {
    .RequiredSharedMemSize = GPRequiredSharedMemSize,
    .InitializeConnectionPool = GPInitializeConnectionPool,
    .LoadChildConnectionPool = GPLoadChildConnectionPool,
    .BorrowClusterConnection = GPBorrowClusterConnection,
    .LoadBorrowedConnection = GPLoadBorrowedConnection,
    .ReleaseClusterConnection = GPReleaseClusterConnection,
    .SyncClusterConnectionDataInPool = GPSyncClusterConnectionDataInPool,
    .PushClusterConnectionToPool = GPPushClusterConnectionToPool,
    .ReleaseChildConnectionPool = GPReleaseChildConnectionPool,
    .ClusterConnectionNeedPush = GPClusterConnectionNeedPush,
    .GetConnectionPoolInfo = GPGetConnectionPoolInfo,
    .GetPoolEntriesCount = GPGetPoolEntriesCount
};

const ConnectionPoolRoutine*
GetGlobalConnectionPool(void)
{
    return &GlobalConnectionPoolRoutine;
}

static ConnectionPoolEntry *
get_pool_entry_for_pool_id(int pool_id)
{
    if (pool_id < 0 || GPGetPoolEntriesCount())
        return NULL;
    return &ConnectionPool[pool_id];
}

/* TODO: Handle disconnected socktes */
static int
get_sockets_array(PooledBackendClusterConnection *backend_endpoint, int **sockets, int *num_sockets, bool pooled_socks)
{
    int i;
    int *socks = NULL;

    if (!backend_endpoint || backend_endpoint->num_sockets <= 0)
        return -1;

    *num_sockets = backend_endpoint->num_sockets;
    socks = palloc(sizeof(int) * *num_sockets);

    for (i = 0; i < *num_sockets; i++)
    {
        int sock_index = backend_endpoint->backend_ids[i];
        if (pooled_socks)
        {
            // if (backend_endpoint->conn_slots[sock_index].socket > 0)
            socks[i] = backend_endpoint->conn_slots[sock_index].socket;
        }
        else
        {
            BackendClusterConnection *current_backend_con = GetBackendClusterConnection();
            socks[i] = current_backend_con->slots[sock_index].con->fd;
        }
    }

    *sockets = socks;
    ereport(DEBUG2, (errmsg("we have %d %s sockets to push", *num_sockets, pooled_socks ? "pooled" : "child")));
    return *num_sockets;
}

/*
 * We should already have the sockets imported from the global pool
 */
static bool
import_pooled_connection_into_child(int pool_id, int *sockets, LEASE_TYPES lease_type)
{
    int i;
    int num_sockets;
    int *backend_ids;
    PooledBackendClusterConnection *backend_end_point = GetGlobalPooledBackendClusterConnection(pool_id);
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    if (backend_end_point == NULL)
        return false;

    ereport(DEBUG2,
            (errmsg("import_pooled_connection_into_child pool_id:%d backend_end_point:%p LeaseType:%d", pool_id, backend_end_point, lease_type)));

    num_sockets = backend_end_point->num_sockets;
    backend_ids = backend_end_point->backend_ids;

    current_backend_con->pool_id = pool_id;
    current_backend_con->backend_end_point = backend_end_point;
    current_backend_con->lease_type = lease_type;

    if (lease_type == LEASE_TYPE_EMPTY_SLOT_RESERVED)
        return true;

    import_pooled_startup_packet_into_child(backend_end_point);

    for (i = 0; i < num_sockets; i++)
    {
        int slot_no = backend_ids[i];
        if (BACKEND_INFO(slot_no).backend_status == CON_DOWN || BACKEND_INFO(slot_no).backend_status == CON_UNUSED)
        {
            /*
             * Although we have received the socket for backend slot
             * but the global status of slot indicates that it is marekd
             * as down. Probably because of failover.
             * So we do not want to use it
             * We also want to mark the slot as down in the global pool
             */
            close(sockets[i]);
            backend_end_point->conn_slots[slot_no].socket = -1;
            backend_end_point->backend_status[i] = BACKEND_INFO(slot_no).backend_status;
            continue;
        }

        current_backend_con->slots[slot_no].con = pool_open(sockets[i], true);
        current_backend_con->slots[slot_no].con->pooled_backend_ref = &backend_end_point->conn_slots[slot_no];

        current_backend_con->slots[slot_no].key = backend_end_point->conn_slots[slot_no].key;
        current_backend_con->slots[slot_no].pid = backend_end_point->conn_slots[slot_no].pid;
        current_backend_con->slots[slot_no].state = CONNECTION_SLOT_LOADED_FROM_BACKEND;
    }

    /* Now take care of backends that were attached after the pool was created */

    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (backend_end_point->backend_status[i] != BACKEND_INFO(i).backend_status)
        {
            if ((backend_end_point->backend_status[i] == CON_DOWN || backend_end_point->backend_status[i] == CON_UNUSED) &&
                (BACKEND_INFO(i).backend_status == CON_UP || BACKEND_INFO(i).backend_status == CON_CONNECT_WAIT))
            {
                /*
                 * Backend was down when the pool was created but now it is up
                 * We need to update connect the backend, push back the socket to global pool
                 * and also sync the status in the global pool
                 */
                ereport(LOG,
                        (errmsg("Backend %d was down when pool was created but now it is up :%d", i, current_backend_con->slots[i].state)));
                if (ConnectBackendSocketForBackendCluster(i) == false)
                {
                    /* set down status to local status area */
                    *(my_backend_status[i]) = CON_DOWN;
                    pool_get_my_process_info()->need_to_restart = 1; // TODO: check if this is needed
                }
                else
                {
                    ereport(LOG, (errmsg("Backend %d was down when pool was created. Sock successfully connected:%d", i,
                                         current_backend_con->slots[i].state)));
                    /* Connection was successfull */
                }
            }
        }
    }
    return true;
}

static bool
export_local_cluster_sockets_pool(void)
{
    int *sockets = NULL;
    int num_sockets = 0;

    get_sockets_array(GetBackendClusterConnection()->backend_end_point, &sockets, &num_sockets, false);
    if (sockets && num_sockets > 0)
    {
        bool ret;
        ret = SendBackendSocktesToMainPool(ParentLinkFd, num_sockets, sockets);
        pfree(sockets);
        return ret;
    }
    else
        ereport(LOG,
                (errmsg("No socket found to transfer to to global connection pool")));

    return true;
}

static bool
export_local_cluster_connection_data_to_pool(void)
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
 ***********************************************
 * Global connection pool server side functions
 ***********************************************
 */

PooledBackendClusterConnection *
GetGlobalPooledBackendClusterConnection(int pool_id)
{
    ConnectionPoolEntry *pool_entry = get_pool_entry_for_pool_id(pool_id);
    if (!pool_entry)
        return NULL;
    return &pool_entry->endPoint;
}

bool
InstallSocketsInConnectionPool(ConnectionPoolEntry *pool_entry, int *sockets)
{
    int i;

    Assert(processType == PT_MAIN)

    if (!pool_entry)
        return false;

    for (i = 0; i < pool_entry->endPoint.num_sockets; i++)
    {
        int slot_no = pool_entry->endPoint.backend_ids[i];
        pool_entry->endPoint.conn_slots[slot_no].socket = sockets[i];
    }
    return true;
}

bool
GlobalPoolReleasePooledConnection(ConnectionPoolEntry *pool_entry, IPC_Endpoint *ipc_endpoint, bool need_cleanup, bool discard)
{
    ProcessInfo *pro_info = NULL;

    if (processType != PT_MAIN)
    {
        ereport(ERROR,
                (errmsg("only main process can unregister new pool lease")));
        return false;
    }

    pro_info = pool_get_process_info_from_IPC_Endpoint(ipc_endpoint);

    if (pool_entry->child_pid != ipc_endpoint->child_pid)
    {
        ereport(WARNING,
                (errmsg("child:%d leased:%d is not the borrower of pool_id:%d borrowed by:%d",
                        ipc_endpoint->child_pid,
                        pro_info->pool_id,
                        pool_entry->pool_id,
                        pool_entry->child_pid)));
        return false;
    }
    if (discard)
    {
        pool_entry->status = POOL_ENTRY_EMPTY;
        memset(&pool_entry->endPoint, 0, sizeof(PooledBackendClusterConnection));
    }
    else
        pool_entry->need_cleanup = need_cleanup;

    unregister_lease(pool_entry->pool_id, ipc_endpoint);
    ereport(LOG,
            (errmsg("child:%d released pool_id:%d database:%s used:%s", ipc_endpoint->child_pid,
                    pool_entry->pool_id,
                    pool_entry->endPoint.database,
                    pool_entry->endPoint.user)));
    return true;
}

void
GlobalPoolChildProcessDied(int child_id, pid_t child_pid)
{
    int i;
    Assert(processType == PT_MAIN);
    Assert(ipc_endpoint);

    for (i = 0; i < GPGetPoolEntriesCount(); i++)
    {
        if (ConnectionPool[i].child_pid == child_pid)
        {
            ConnectionPool[i].child_pid = -1;
            ConnectionPool[i].child_id = -1;
            ConnectionPool[i].need_cleanup = true;
            ConnectionPool[i].status = POOL_ENTRY_EMPTY;
            ConnectionPool[i].last_returned_time = time(NULL);
            ConnectionPool[i].status = POOL_ENTRY_EMPTY;
            memset(&ConnectionPool[i].endPoint, 0, sizeof(PooledBackendClusterConnection));
            break; /* Only one connection can be leased to any child */
        }
    }
}

bool
GlobalPoolLeasePooledConnectionToChild(IPC_Endpoint *ipc_endpoint)
{
    ProcessInfo *child_proc_info;

    LEASE_TYPES lease_type = LEASE_TYPE_LEASE_FAILED;
    int pool_id = -1;
    bool ret;

    if (processType != PT_MAIN)
        return false;

    if (ipc_endpoint->proc_info_id < 0 || ipc_endpoint->proc_info_id >= pool_config->num_init_children)
        return false;
    child_proc_info = &process_info[ipc_endpoint->proc_info_id];

    if (child_proc_info->pid != ipc_endpoint->child_pid)
        return false;

    pool_id = get_pooled_connection_for_lending(child_proc_info->user,
                                                child_proc_info->database,
                                                child_proc_info->major,
                                                &lease_type);
    if (pool_id >= 0)
    {
        ereport(DEBUG2,
                (errmsg("pool_id:%d with lease type:%d reserved for child:%d", pool_id, lease_type, ipc_endpoint->child_pid)));

        ret = register_new_lease(pool_id, lease_type, ipc_endpoint);
        if (ret == false)
        {
            ereport(LOG,
                    (errmsg("Failed to register (pool_id:%d) lease type:%d to child:%d", pool_id, lease_type, ipc_endpoint->child_pid)));
            lease_type = LEASE_TYPE_LEASE_FAILED;
        }
    }
    else
    {
        ereport(LOG,
                (errmsg("No pooled connection for user:%s database:%s protoMajor:%d", child_proc_info->user,
                        child_proc_info->database,
                        child_proc_info->major)));
    }

    ret = InformLeaseStatusToChild(ipc_endpoint->child_link, lease_type);
    if (ret == false)
    {
        ereport(LOG,
                (errmsg("Failed to send (pool_id:%d) lease type:%d to child:%d", pool_id, lease_type, ipc_endpoint->child_pid)));
        unregister_lease(pool_id, ipc_endpoint);
        return false;
    }

    if (lease_type == LEASE_TYPE_READY_TO_USE || lease_type == LEASE_TYPE_DISCART_AND_CREATE)
    {
        int *sockets = NULL;
        int num_sockets = 0;
        get_sockets_array(&ConnectionPool[pool_id].endPoint, &sockets, &num_sockets, true);
        if (sockets && num_sockets > 0)
        {
            ret = TransferSocketsBetweenProcesses(ipc_endpoint->child_link, num_sockets, sockets);
            pfree(sockets);
            if (ret == false)
                unregister_lease(pool_id, ipc_endpoint);
            return ret;
        }
        else
            ereport(LOG,
                    (errmsg("No socket found to transfer to to child:%d", ipc_endpoint->child_pid)));
    }

    return ret;
}

static int
get_pooled_connection_for_lending(char *user, char *database, int protoMajor, LEASE_TYPES *lease_type)
{
    int i;
    int free_pool_slot = -1;
    int discard_pool_slot = -1;
    bool found_good_victum = false;
    time_t closetime = TMINTMAX;

    ereport(DEBUG2,
            (errmsg("Finding for user:%s database:%s protoMajor:%d", user, database, protoMajor)));

    for (i = 0; i < GPGetPoolEntriesCount(); i++)
    {
        ereport(DEBUG2,
                (errmsg("POOL:%d, STATUS:%d [%s:%s]", i, ConnectionPool[i].status,
                        ConnectionPool[i].endPoint.database,
                        ConnectionPool[i].endPoint.user)));

        if (ConnectionPool[i].status == POOL_ENTRY_CONNECTED &&
            ConnectionPool[i].child_pid <= 0 &&
            ConnectionPool[i].endPoint.sp.major == protoMajor)
        {
            if (strcmp(ConnectionPool[i].endPoint.user, user) == 0 &&
                strcmp(ConnectionPool[i].endPoint.database, database) == 0)
            {
                if (ConnectionPool[i].need_cleanup)
                    *lease_type = LEASE_TYPE_DISCART_AND_CREATE;
                else
                    *lease_type = LEASE_TYPE_READY_TO_USE;
                return i;
            }
        }
        
        if (free_pool_slot == -1 && ConnectionPool[i].status == POOL_ENTRY_EMPTY && ConnectionPool[i].child_pid <= 0)
            free_pool_slot = i;
        /* Also try calculating the victum in case we failed to find even a free connection */
        if (free_pool_slot == -1 && found_good_victum == false)
        {
            if (ConnectionPool[i].status == POOL_ENTRY_CONNECTED &&
                ConnectionPool[i].child_pid <= 0)
            {
                if (ConnectionPool[i].need_cleanup)
                {
                    discard_pool_slot = i;
                    found_good_victum = true;
                }
                else if (ConnectionPool[i].last_returned_time < closetime)
                {
                    closetime = ConnectionPool[i].last_returned_time;
                    discard_pool_slot = i;
                }
            }
        }
    }

    /* No pooled connection found. Return empty slot */
    if (free_pool_slot > -1)
    {
        *lease_type = LEASE_TYPE_EMPTY_SLOT_RESERVED;
        return free_pool_slot;
    }
    else if (discard_pool_slot > -1)
    {
        *lease_type = LEASE_TYPE_DISCART_AND_CREATE;
        return discard_pool_slot;
    }
    /* TODO Find the pooled entry that can be discarded and re-used */
    ereport(LOG,
            (errmsg("No free slot available for user:%s database:%s protoMajor:%d", user, database, protoMajor)));

    /* Nothing found */
    *lease_type = LEASE_TYPE_NO_AVAILABLE_SLOT;
    return -1;
}


static bool
unregister_lease(int pool_id, IPC_Endpoint *ipc_endpoint)
{
    ProcessInfo *child_proc_info;

    if (processType != PT_MAIN)
    {
        ereport(ERROR,
                (errmsg("only main process can unregister new pool lease")));
        return false;
    }
    if (pool_id < 0 || pool_id >= GPGetPoolEntriesCount())
    {
        ereport(ERROR,
                (errmsg("pool_id:%d is out of range", pool_id)));
        return false;
    }
    if (ConnectionPool[pool_id].child_pid > 0 && ConnectionPool[pool_id].child_pid != ipc_endpoint->child_pid)
    {
        ereport(ERROR,
                (errmsg("pool_id:%d is leased to different child:%d", pool_id, ConnectionPool[pool_id].child_pid)));
        return false;
    }
    child_proc_info = &process_info[ipc_endpoint->proc_info_id];
    child_proc_info->pool_id = -1;

    ereport(DEBUG1,
            (errmsg("pool_id:%d, is released from child:%d", pool_id, ConnectionPool[pool_id].child_pid)));

    ConnectionPool[pool_id].child_pid = -1;
    ConnectionPool[pool_id].child_id = -1;
    ConnectionPool[pool_id].endPoint.client_disconnection_time = time(NULL);
    ConnectionPool[pool_id].endPoint.client_disconnection_count++;

    return true;
}

static bool
register_new_lease(int pool_id, LEASE_TYPES lease_type, IPC_Endpoint *ipc_endpoint)
{
    ProcessInfo *child_proc_info;

    if (processType != PT_MAIN)
    {
        ereport(ERROR,
                (errmsg("only main process can register new pool lease")));
        return false;
    }
    if (pool_id < 0 || pool_id >= GPGetPoolEntriesCount())
    {
        ereport(ERROR,
                (errmsg("pool_id:%d is out of range", pool_id)));
        return false;
    }
    if (ConnectionPool[pool_id].child_pid > 0 && ConnectionPool[pool_id].child_pid != ipc_endpoint->child_pid)
    {
        ereport(ERROR,
                (errmsg("pool_id:%d is already leased to child:%d", pool_id, ConnectionPool[pool_id].child_pid)));
        return false;
    }

    ConnectionPool[pool_id].child_pid = ipc_endpoint->child_pid;
    ConnectionPool[pool_id].child_id = ipc_endpoint->proc_info_id;
    child_proc_info = &process_info[ipc_endpoint->proc_info_id];
    child_proc_info->pool_id = pool_id;
    if (ConnectionPool[pool_id].status == POOL_ENTRY_CONNECTED)
    {
        ConnectionPool[pool_id].leased_count++;
        ConnectionPool[pool_id].leased_time = time(NULL);
    }
    ereport(LOG,
            (errmsg("pool_id:%d, leased to child:%d", pool_id, ConnectionPool[pool_id].child_pid)));
    return true;
}
