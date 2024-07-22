#include "pool.h"
#include "pool_config.h"
#include "connection_pool/connection_pool.h"
#include "utils/elog.h"

static const ConnectionPoolRoutine *activeConnectionPool = NULL;
ConnectionPoolEntry *ConnectionPool = NULL;

static PooledBackendClusterConnection* get_backend_connection_for_cancel_packer(CancelPacket *cp);
static PooledBackendNodeConnection *get_backend_node_connection_for_backend_pid(int backend_pid, int *backend_node_id);

void
InstallConnectionPool(const ConnectionPoolRoutine *ConnectionPoolRoutine)
{
    Assert(processType == PT_MAIN);
    if (activeConnectionPool)
    {
        ereport(ERROR,
                (errmsg("Connection pool routine already installed")));
    }
    activeConnectionPool = ConnectionPoolRoutine;
}

size_t
ConnectionPoolRequiredSharedMemSize(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_MAIN);
    if (activeConnectionPool->RequiredSharedMemSize)
        return activeConnectionPool->RequiredSharedMemSize();
    return 0;
}
int
GetPoolEntriesCount(void)
{
    Assert(activeConnectionPool);
    return activeConnectionPool->GetPoolEntriesCount();
}

const char*
GetConnectionPoolInfo(void)
{
    if (! activeConnectionPool)
        return "No connection pool installed";
    Assert(activeConnectionPool->GetConnectionPoolInfo)
    return activeConnectionPool->GetConnectionPoolInfo();
}

void
InitializeConnectionPool(void *shared_mem_ptr)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_MAIN);
    activeConnectionPool->InitializeConnectionPool(shared_mem_ptr);
}

void
LoadChildConnectionPool(int intarg)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    activeConnectionPool->LoadChildConnectionPool(intarg);
}

BorrowConnectionRes*
BorrowClusterConnection(char *database, char *user, int major, int minor)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);

    return activeConnectionPool->BorrowClusterConnection(database, user, major, minor);
}

bool
LoadBorrowedConnection(BorrowConnectionRes *context)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->LoadBorrowedConnection(context);
}

bool
ReleaseClusterConnection(bool discard)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->ReleaseClusterConnection(discard);
}

bool
PushClusterConnectionToPool(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->PushClusterConnectionToPool();
}

bool
SyncClusterConnectionDataInPool(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->SyncClusterConnectionDataInPool();
}

void
ReleaseChildConnectionPool(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    activeConnectionPool->ReleaseChildConnectionPool();
}

PooledBackendNodeConnection *
GetBackendNodeConnectionForBackendPID(int backend_pid, int *backend_node_id)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    if (activeConnectionPool->GetBackendNodeConnectionForBackendPID)
        return activeConnectionPool->GetBackendNodeConnectionForBackendPID(backend_pid, backend_node_id);
    return get_backend_node_connection_for_backend_pid(backend_pid, backend_node_id);
}
PooledBackendClusterConnection *
GetBackendEndPointForCancelPacket(CancelPacket *cp)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);

    if (activeConnectionPool->GetBackendEndPointForCancelPacket)
        return activeConnectionPool->GetBackendEndPointForCancelPacket(cp);
    
    return get_backend_connection_for_cancel_packer(cp);
}

bool ClusterConnectionNeedPush(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->ClusterConnectionNeedPush();
}

/* Functions that work on any installed connection pool */
static PooledBackendClusterConnection *
get_backend_connection_for_cancel_packer(CancelPacket *cp)
{
    int i;
    Assert(activeConnectionPool);
    Assert(ConnectionPool);
    Assert(processType == PT_CHILD);

    for (i = 0; i < GetPoolEntriesCount(); i++)
    {
        int con_slot;
        if (ConnectionPool[i].status == POOL_ENTRY_EMPTY)
        {
            elog(DEBUG2, "get_backend_connection_for_cancel_packer: empty entry [%d] child_id:%d, pool_id%d, status:%d, child_pid:%d", i,
                 ConnectionPool[i].child_id,
                 ConnectionPool[i].pool_id,
                 ConnectionPool[i].status,
                 ConnectionPool[i].child_pid);
            continue;
        }

        for (con_slot = 0; con_slot < NUM_BACKENDS; con_slot++)
        {
            PooledBackendNodeConnection *c = &ConnectionPool[i].endPoint.conn_slots[con_slot];

            if (!VALID_BACKEND(con_slot))
                continue;

            ereport(LOG,
                    (errmsg("processing cancel request"),
                     errdetail("connection info: database:%s user:%s pid:%d key:%d i:%d",
                               ConnectionPool[i].endPoint.database, ConnectionPool[i].endPoint.user,
                               ntohl(c->pid), ntohl(c->key), con_slot)));
            if (c->pid == cp->pid && c->key == cp->key)
            {
                ereport(DEBUG1,
                        (errmsg("processing cancel request"),
                         errdetail("found pid:%d key:%d i:%d", ntohl(c->pid), ntohl(c->key), con_slot)));
                return &ConnectionPool[i].endPoint;
            }
        }
    }
    return NULL;
}

/*
 * locate and return the shared memory BackendConnection having the
 * backend connection with the pid
 * If the connection is found the *backend_node_id contains the backend node id
 * of the backend node that has the connection
 */
static PooledBackendNodeConnection *
get_backend_node_connection_for_backend_pid(int backend_pid, int *backend_node_id)
{
    int i;
    Assert(activeConnectionPool);
    Assert(ConnectionPool);
    Assert(processType == PT_CHILD);

    for (i = 0; i < GetPoolEntriesCount(); i++)
    {
        int con_slot;
        if (ConnectionPool[i].status == POOL_ENTRY_EMPTY)
            continue;
        for (con_slot = 0; con_slot < NUM_BACKENDS; con_slot++)
        {
            if (!VALID_BACKEND(con_slot))
                continue;

            if (ConnectionPool[i].endPoint.conn_slots[con_slot].pid == backend_pid)
            {
                *backend_node_id = i;
                return &ConnectionPool[i].endPoint.conn_slots[con_slot];
            }
        }
    }
    return NULL;
}