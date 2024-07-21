#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include "pool.h"
#include "connection_pool/backend_connection.h"


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
    int pool_id;
    pid_t borrower_pid;
    int borrower_proc_info_id;
    bool need_cleanup;
    time_t leased_time;
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
    PooledBackendNodeConnection *(*GetBackendNodeConnectionForBackendPID)(int backend_pid, int *backend_node_id);
    PooledBackendClusterConnection* (*GetBackendEndPointForCancelPacket) (CancelPacket *cp);
    bool (*ClusterConnectionNeedPush)(void);
} ConnectionPoolRoutine;

extern void InstallConnectionPool(const ConnectionPoolRoutine *ConnectionPoolRoutine);

extern size_t ConnectionPoolRequiredSharedMemSize(void);
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

/* from global_connection_pool.c */
extern const ConnectionPoolRoutine *GetGlobalConnectionPool(void);
extern ConnectionPoolEntry *GetGlobalConnectionPoolEntry(int pool_id);
extern bool InstallSocketsInConnectionPool(ConnectionPoolEntry *pool_entry, int *sockets);
extern PooledBackendClusterConnection *GetGlobalPooledBackendClusterConnection(int pool_id);
extern bool GlobalPoolReleasePooledConnection(ConnectionPoolEntry *pool_entry, IPC_Endpoint *ipc_endpoint, bool need_cleanup, bool discard);
extern bool GlobalPoolLeasePooledConnectionToChild(IPC_Endpoint *ipc_endpoint);

/* from classic_connection_pool.c */
extern const ConnectionPoolRoutine *GetClassicConnectionPool(void);
#endif // CONNECTION_POOL_H
