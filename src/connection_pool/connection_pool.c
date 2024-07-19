#include "pool.h"
#include "connection_pool/connection_pool.h"
#include "utils/elog.h"

static const ConnectionPoolRoutine *activeConnectionPool = NULL;

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
    Assert(processType == PT_MAIN);
    return activeConnectionPool->GetBackendNodeConnectionForBackendPID(backend_pid, backend_node_id);
}
PooledBackendClusterConnection *
GetBackendEndPointForCancelPacket(CancelPacket *cp)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_MAIN);
    return activeConnectionPool->GetBackendEndPointForCancelPacket(cp);
}

    bool ClusterConnectionNeedPush(void)
{
    Assert(activeConnectionPool);
    Assert(processType == PT_CHILD);
    return activeConnectionPool->ClusterConnectionNeedPush();
}
