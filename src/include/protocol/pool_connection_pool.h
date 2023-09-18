/*
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2020	PgPool Global Development Group
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


#ifndef pool_connection_pool_h
#define pool_connection_pool_h

#include "pool.h"

extern ConnectionPoolEntry	*ConnectionPool;

extern void pool_init_cp(int parent_link_fd);

bool DiscardBackendConnection(bool release_pool);
bool ClearChildPooledConnectio(void);

extern ChildBackendConnection* GetChildBackendConnection(void);
extern bool ConnectBackendSocktes(void);
extern void pool_discard_cp(char *user, char *database, int protoMajor);
extern void pool_backend_timer(void);
extern void pool_connection_pool_timer(POOL_CONNECTION_POOL * backend);
extern RETSIGTYPE pool_backend_timer_handler(int sig);
extern int	connect_inet_domain_socket(int slot, bool retry);
extern int	connect_unix_domain_socket(int slot, bool retry);
extern int	connect_inet_domain_socket_by_port(char *host, int port, bool retry);
extern int	connect_unix_domain_socket_by_port(int port, char *socket_dir, bool retry);
extern int	pool_pool_index(void);
extern void close_all_backend_connections(void);
extern void update_pooled_connection_count(void);


/* Global connection pool */
extern ConnectionPoolEntry* GetConnectionPool(void);
extern BackendEndPoint* GetChildBorrowedBackendEndPoint(void);
extern bool StorePasswordInformation(char* password, int pwd_size, PasswordType passwordType);
extern bool SaveAuthKindForBackendConnection(int auth_kind);
extern int GetAuthKindForCurrentPoolBackendConnection(void);

extern size_t get_global_connection_pool_shared_mem_size(void);
extern void init_global_connection_pool(void);
extern bool InitializeChildLocalBackendConnection(int pool_id, StartupPacket* sp);
extern bool ImportPoolConnectionIntoChild(int pool_id, int *sockets, LEASE_TYPES lease_type);
extern bool LeasePooledConnectionToChild(IPC_Endpoint* ipc_endpoint);
extern int GetPooledConnectionForLending(char *user, char *database, int protoMajor, LEASE_TYPES *lease_type);
extern bool ExportLocalBackendConnectionToPool(void);
extern bool ExportLocalSocketsToBackendPool(void);
extern bool InstallSocketsInConnectionPool(ConnectionPoolEntry* pool_entry, int *sockets);
extern BackendEndPoint* GetBackendEndPoint(int pool_id);
extern ConnectionPoolEntry* GetConnectionPoolEntry(int pool_id);
extern ConnectionPoolEntry* GetChildConnectionPoolEntry(void);
extern bool ChildBackendConnectionNeedPush(void);
extern bool ReleasePooledConnection(ConnectionPoolEntry* pool_entry, IPC_Endpoint* ipc_endpoint, bool need_cleanup, bool discard);

extern BackendConnection* GetBackendConnectionForBackendPID(int backend_pid, int *backend_node_id);
extern BackendEndPoint* GetBackendEndPointForCancelPacket(CancelPacket* cp);
extern void ClearChildBackendConnection(void);

// #define CONNECTION_SLOT(slot) ((GetChildBackendConnection())->slots[(slot)])
// #define CONNECTION(slot) (CONNECTION_SLOT(slot)->connection)
// #define MAIN_CONNECTION() ((GetChildBackendConnection())->slots[MAIN_NODE_ID])
// #define MAIN() MAIN_CONNECTION()->connection

#endif /* pool_connection_pool_h */
