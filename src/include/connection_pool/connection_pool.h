#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include "pool.h"
#include "connection_pool/backend_connection.h"

#define TMINTMAX 0x7fffffff

typedef enum POOL_ENTRY_STATUS
{
    POOL_ENTRY_EMPTY = 0,
    POOL_ENTRY_LOADED, /* Intermediate status when pool info is populated
                        * by child process but sockets are yet to be transfered */
    POOL_ENTRY_CONNECTED
} POOL_ENTRY_STATUS;

/* This structure crossponds to the backend connection
 * in the global connection pool.
 */
typedef struct ConnectionPoolEntry
{
    PooledBackendClusterConnection endPoint;
    POOL_ENTRY_STATUS status;
    pid_t child_pid;
    int pool_id;
    int child_id;
    bool need_cleanup;
    time_t leased_time;
    time_t last_returned_time;
    int leased_count;
} ConnectionPoolEntry;

typedef struct BorrowConnectionRes
{
    LEASE_TYPES lease_type;
    int pool_id;
    void *context;
} BorrowConnectionRes;

typedef struct ConnectionPoolRoutine
{
    size_t (*RequiredSharedMemSize)(void);
    void (*InitializeConnectionPool) (void* shared_mem_ptr);
    void (*LoadChildConnectionPool)(int intarg);
    BorrowConnectionRes* (*BorrowClusterConnection)(char *database, char *user, int major, int minor);
    bool (*LoadBorrowedConnection)(BorrowConnectionRes *context);
    bool (*ReleaseClusterConnection) (bool discard);
    bool (*SyncClusterConnectionDataInPool) (void);
    bool (*PushClusterConnectionToPool) (void);
    void (*ReleaseChildConnectionPool)  (void);
    PooledBackendNodeConnection *(*GetBackendNodeConnectionForBackendPID)(int backend_pid, int *backend_node_id); /* Optional overload */
    PooledBackendClusterConnection* (*GetBackendEndPointForCancelPacket) (CancelPacket *cp); /* optional overload */
    bool (*ClusterConnectionNeedPush)(void);
    const char *(*GetConnectionPoolInfo)(void);
    void (*UpdatePooledConnectionCount)(void);
    int (*GetPoolEntriesCount)(void);
    ConnectionPoolEntry *(*GetConnectionPoolEntry)(int pool_idx, int child_id);
} ConnectionPoolRoutine;

extern void InstallConnectionPool(const ConnectionPoolRoutine *ConnectionPoolRoutine);
extern int GetPoolEntriesCount(void);
extern size_t ConnectionPoolRequiredSharedMemSize(void);
extern const char *GetConnectionPoolInfo(void);
extern void InitializeConnectionPool(void *shared_mem_ptr);
extern void LoadChildConnectionPool(int intarg);
extern BorrowConnectionRes *BorrowClusterConnection(char *database, char *user, int major, int minor);
extern bool LoadBorrowedConnection(BorrowConnectionRes *context);
extern bool ReleaseClusterConnection(bool discard);
extern bool PushClusterConnectionToPool(void);
extern bool SyncClusterConnectionDataInPool(void);
extern void ReleaseChildConnectionPool(void);
extern bool ClusterConnectionNeedPush(void);
extern PooledBackendNodeConnection *GetBackendNodeConnectionForBackendPID(int backend_pid, int *backend_node_id);
extern PooledBackendClusterConnection *GetBackendEndPointForCancelPacket(CancelPacket *cp);
extern ConnectionPoolEntry *GetConnectionPoolEntryAtIndex(int pool_idx);
extern ConnectionPoolEntry *GetConnectionPoolEntry(int pool_id, int child_id);
extern void UpdatePooledConnectionCount(void);

extern bool ConnectionPoolRegisterNewLease(ConnectionPoolEntry *pool_entry, LEASE_TYPES lease_type, int child_id, pid_t child_pid);
extern bool ConnectionPoolUnregisterLease(ConnectionPoolEntry* pool_entry, int child_id, pid_t child_pid);

extern int InvalidateAllPooledConnections(char *database);
extern int InvalidateNodeInPooledConnections(int node_id);

/* from global_connection_pool.c */
extern const ConnectionPoolRoutine *GetGlobalConnectionPool(void);
extern bool InstallSocketsInConnectionPool(ConnectionPoolEntry *pool_entry, int *sockets);
extern PooledBackendClusterConnection *GetGlobalPooledBackendClusterConnection(int pool_id);
extern bool GlobalPoolReleasePooledConnection(ConnectionPoolEntry *pool_entry, IPC_Endpoint *ipc_endpoint, bool need_cleanup, bool discard);
extern bool GlobalPoolLeasePooledConnectionToChild(IPC_Endpoint *ipc_endpoint);
extern void GlobalPoolChildProcessDied(int child_id, pid_t child_pid);
/* from classic_connection_pool.c */
extern const ConnectionPoolRoutine *GetClassicConnectionPool(void);
#endif // CONNECTION_POOL_H
