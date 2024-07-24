#ifndef BACKEND_CONNECTION_H
#define BACKEND_CONNECTION_H

#include "pool.h"

typedef struct PooledBackendNodeConnection
{
    // ConnectionInfo conn_info;
    int pid;            /* backend process id */
    int key;            /* cancel key */
    int counter;        /* used counter */
    time_t create_time; /* connection creation time */
    int socket;
    bool connected;
    volatile bool need_reconnect;
    char salt[4]; /* password salt */

    volatile bool swallow_termination;
} PooledBackendNodeConnection;

typedef struct PooledBackendClusterConnection
{
    char database[SM_DATABASE]; /* Database name */
    char user[SM_USER];         /* User name */
    ParamStatus params;
    int load_balancing_node;
    char startup_packet_data[MAX_STARTUP_PACKET_LENGTH]; /* startup packet info */
    StartupPacket sp;                                    /* startup packet info */
    // int key;                                             /* cancel key */
    /*
     * following are used to remember when re-use the authenticated connection
     */
    int auth_kind;                        /* 3: clear text password, 4: crypt password,
                                           * 5: md5 password */
    int pwd_size;                         /* password (sent back from frontend) size in
                                           * host order */
    char password[MAX_PASSWORD_SIZE + 1]; /* password (sent back
                                           * from frontend) */
    PasswordType passwordType;

    time_t client_connection_time;
    time_t client_disconnection_time;
    bool client_connected;
    int client_connection_count;
    int client_disconnection_count;

    int num_sockets;
    int backend_ids[MAX_NUM_BACKENDS];
    PooledBackendNodeConnection conn_slots[MAX_NUM_BACKENDS];
    BACKEND_STATUS backend_status[MAX_NUM_BACKENDS]; /* Backend status of each node*/
    int primary_node_id;                             /* the primary node id in streaming
                                                      * replication mode at the time of connection*/
    volatile sig_atomic_t node_status_changed;
    time_t node_status_last_changed_time;

} PooledBackendClusterConnection;

/*
 * stream connection structure
 */
typedef struct
{
    int fd; /* fd for connection */

    char *wbuf; /* write buffer for the connection */
    int wbufsz; /* write buffer size */
    int wbufpo; /* buffer offset */

#ifdef USE_SSL
    SSL_CTX *ssl_ctx; /* SSL connection context */
    SSL *ssl;         /* SSL connection */
    X509 *peer;
    char *cert_cn; /* common in the ssl certificate presented by
                    * frontend connection Used for cert
                    * authentication */
    bool client_cert_loaded;

#endif
    int ssl_active; /* SSL is failed if < 0, off if 0, on if > 0 */

    char *hp;  /* pending data buffer head address */
    int po;    /* pending data offset */
    int bufsz; /* pending data buffer size */
    int len;   /* pending data length */

    char *sbuf; /* buffer for pool_read_string */
    int sbufsz; /* its size in bytes */

    char *buf2; /* buffer for pool_read2 */
    int bufsz2; /* its size in bytes */

    char *buf3; /* buffer for pool_push/pop */
    int bufsz3; /* its size in bytes */

    int isbackend;  /* this connection is for backend if non 0 */
    int db_node_id; /* DB node id for this connection */

    char tstate; /* Transaction state (V3 only) 'I' if idle
                  * (not in a transaction block); 'T' if in a
                  * transaction block; or 'E' if in a failed
                  * transaction block */

    /* True if an internal transaction has already started */
    bool is_internal_transaction_started;

    /*
     * following are used to remember when re-use the authenticated connection
     */
    int auth_kind;                        /* 3: clear text password, 4: crypt password,
                                           * 5: md5 password */
    int pwd_size;                         /* password (sent back from frontend) size in
                                           * host order */
    char password[MAX_PASSWORD_SIZE + 1]; /* password (sent back
                                           * from frontend) */
    char salt[4];                         /* password salt */
    PasswordType passwordType;

    /*
     * following are used to remember current session parameter status.
     * re-used connection will need them (V3 only)
     */
    //	ParamStatus params;

    int no_forward; /* if non 0, do not write to frontend */

    char kind; /* kind cache */

    /* true if remote end closed the connection */
    POOL_SOCKET_STATE socket_state;

    /*
     * frontend info needed for hba
     */
    int protoVersion;
    SockAddr raddr;
    HbaLine *pool_hba;
    char *database;
    char *username;
    char *remote_hostname;
    int remote_hostname_resolv;
    bool frontend_authenticated;
    PasswordMapping *passwordMapping;
    PooledBackendNodeConnection *pooled_backend_ref; /* points to shared mem */
} POOL_CONNECTION;

typedef enum BACKEND_CONNECTION_STATES
{
    CONNECTION_SLOT_EMPTY = 0,
    CONNECTION_SLOT_LOADED_FROM_BACKEND,
    CONNECTION_SLOT_SOCKET_CONNECTION_ERROR,
    CONNECTION_SLOT_SOCKET_CONNECTION_ONLY,
    CONNECTION_SLOT_VALID_LOCAL_CONNECTION,
    CONNECTION_SLOT_AUTHENTICATION_OK
} BACKEND_CONNECTION_STATES;

typedef struct BackendNodeConnection
{
    int pid;          /* backend pid */
    int key;          /* cancel key */
    time_t closetime; /* absolute time in second when the connection
                       * closed if 0, that means the connection is
                       * under use. */
    BACKEND_CONNECTION_STATES state;
    POOL_CONNECTION *con;
} BackendNodeConnection;

typedef struct BackendClusterConnection
{
    StartupPacket *sp;                  /* startup packet info */
    LEASE_TYPES lease_type;             /* lease type */
    PooledBackendClusterConnection *backend_end_point; /* Reference to global pool end point in shared mem */
    int pool_id;                        /* global pool id */
    // bool need_push_back;                /* true if this connection needs to be pushed back to global pool */
    BackendNodeConnection slots[MAX_NUM_BACKENDS];
} BackendClusterConnection;

extern BackendClusterConnection* GetBackendClusterConnection(void);
extern bool ConnectBackendClusterSockets(void);
extern void ResetBackendClusterConnection(void);
extern PooledBackendClusterConnection* GetCurrentPooledBackendClusterConnection(void);
extern bool StorePasswordInformation(char *password, int pwd_size, PasswordType passwordType);
extern bool SaveAuthKindForBackendConnection(int auth_kind);
extern int GetAuthKindForCurrentPoolBackendConnection(void);
extern void ImportStartupPacketIntoChild(StartupPacket *sp, char *startup_packet_data);
extern bool ConnectBackendSocketForBackendCluster(int slot_no);

extern int check_socket_status(int fd);
    /* in pgpool_main.c */
    extern POOL_NODE_STATUS *
    verify_backend_node_status(BackendNodeConnection **slots);
/* worker_child*/
extern int get_query_result(BackendNodeConnection **slots, int backend_id, char *query, POOL_SELECT_RESULT **res);

#endif // BACKEND_CONNECTION_H
