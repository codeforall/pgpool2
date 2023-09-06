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

typedef enum POOL_ENTRY_STATUS
{
	POOL_ENTRY_EMPTY = 0,
	POOL_ENTRY_LEASED,
	POOL_ENTRY_READY,
	POOL_ENTRY_RESERVED
}			POOL_ENTRY_STATUS;

typedef struct BackendConnection
{
	ConnectionInfo conn_info;
	int			pid;			/* backend process id */
	int			key;			/* cancel key */
	int			counter;		/* used counter */
	time_t		create_time;	/* connection creation time */
	int 		socket;
    bool        connected;

	/*
	 * following are used to remember when re-use the authenticated connection
	 */
	int			auth_kind;		/* 3: clear text password, 4: crypt password,
								 * 5: md5 password */
	int			pwd_size;		/* password (sent back from frontend) size in
								 * host order */
	char		password[MAX_PASSWORD_SIZE + 1];	/* password (sent back
													 * from frontend) */
	char		salt[4];		/* password salt */
	PasswordType passwordType;

	volatile bool swallow_termination;
}BackendConnection;

typedef struct
{
	char		database[SM_DATABASE];	/* Database name */
	char		user[SM_USER];	/* User name */
    int         load_balancing_node;
	char        startup_packet_data[MAX_STARTUP_PACKET_LENGTH];			/* startup packet info */
	StartupPacket sp;			/* startup packet info */
	int			key;			/* cancel key */
    int         auth_kind;
    int			num_sockets;
    int         backend_ids[MAX_NUM_BACKENDS];
	BackendConnection conn_slots[MAX_NUM_BACKENDS];
    
}			BackendEndPoint;

typedef struct
{
	BackendEndPoint 	endPoint;
	POOL_ENTRY_STATUS	status;
    int     pool_id;
	pid_t	borrower_pid;
	time_t 	leased_time;
	int		leased_count;
}	ConnectionPoolEntry;

typedef enum LEASE_TYPES
{
	LEASE_TYPE_INVALID,
	LEASE_TYPE_READY_TO_USE,
	LEASE_TYPE_DISCART_AND_CREATE,
	LEASE_TYPE_EMPTY_SLOT_RESERVED,
	LEASE_TYPE_NO_AVAILABLE_SLOT,
	LEASE_TYPE_NON_POOL_CONNECTION
} LEASE_TYPES;

typedef struct ChildBackendConnectionSlot
{
	int			pid;			/* backend pid */
	int			key;			/* cancel key */
	POOL_CONNECTION *connection;
}			ChildBackendConnectionSlot;

typedef struct ChildBackendConnection
{
	StartupPacket 		*sp;			/* startup packet info */
	bool 				borrowed;		/* true if borrowed from global pool */
	BackendEndPoint*	backend_end_point; /* Reference to global pool end point in shared mem */
	int					pool_id;		/* global pool id */
	ChildBackendConnectionSlot slots[MAX_NUM_BACKENDS];
} 			ChildBackendConnection;

extern ConnectionPoolEntry	*ConnectionPool;

extern POOL_CONNECTION_POOL * pool_connection_pool; /* connection pool */

extern void pool_init_cp(int parent_link_fd);
extern ChildBackendConnection* GetChildBackendConnection(void);
extern bool ConnectBackendSocktes(void);
extern POOL_CONNECTION_POOL * pool_get_cp(char *user, char *database, int protoMajor, int check_socket);
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


extern size_t get_global_connection_pool_shared_mem_size(void);
extern void init_global_connection_pool(void);

extern bool ImportPoolConnectionIntoChild(int pool_id, int *sockets);;
extern bool LeasePooledConnectionToChild(IPC_Endpoint* ipc_endpoint);
extern int GetPooledConnectionForLending(char *user, char *database, int protoMajor, LEASE_TYPES *lease_type);
extern bool ExportLocalBackendConnectionToPool(void);
extern bool InstallSocketsInConnectionPool(int pool_id, int *sockets, int num_sockets, int *slot_ids);
extern BackendEndPoint* GetBackendEndPoint(int pool_id);


// #define CONNECTION_SLOT(slot) ((GetChildBackendConnection())->slots[(slot)])
// #define CONNECTION(slot) (CONNECTION_SLOT(slot)->connection)
// #define MAIN_CONNECTION() ((GetChildBackendConnection())->slots[MAIN_NODE_ID])
// #define MAIN() MAIN_CONNECTION()->connection

#endif /* pool_connection_pool_h */
