#include "connection_pool/backend_connection.h"
#include "connection_pool/connection_pool.h"
#include "protocol/pool_connection_pool.h"
#include "protocol/pool_process_query.h"

#include "context/pool_process_context.h"
#include "main/pool_internal_comms.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/pool_stream.h"
#include "pool_config.h"
#include "utils/elog.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <sys/select.h>

BackendClusterConnection clusterConnection = {.backend_end_point = NULL, .sp = NULL, .lease_type = LEASE_TYPE_FREE, .pool_id = -1};

bool
ConnectBackendSocketForBackendCluster(int slot_no);

void
ResetBackendClusterConnection(void)
{
    int i;
    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (clusterConnection.slots[i].con)
            pool_close(clusterConnection.slots[i].con, false);
    }
    /* Free Startup Packet */
    if (clusterConnection.sp)
    {
        if (clusterConnection.sp->database)
            pfree(clusterConnection.sp->database);
        if (clusterConnection.sp->user)
            pfree(clusterConnection.sp->user);

        pfree(clusterConnection.sp->startup_packet);
        pfree(clusterConnection.sp);
        clusterConnection.sp->database = NULL;
        clusterConnection.sp->user = NULL;
        clusterConnection.sp = NULL;
    }

    clusterConnection.backend_end_point = NULL;
    clusterConnection.lease_type = LEASE_TYPE_FREE;
    clusterConnection.pool_id = -1;
    memset(clusterConnection.slots, 0, sizeof(BackendNodeConnection) * MAX_NUM_BACKENDS);
}

BackendClusterConnection *
GetBackendClusterConnection(void)
{
    return &clusterConnection;
}

PooledBackendClusterConnection *
GetCurrentPooledBackendClusterConnection(void)
{
    if (processType != PT_CHILD)
        return NULL;
    return clusterConnection.backend_end_point;
}

bool
StorePasswordInformation(char *password, int pwd_size, PasswordType passwordType)
{
    PooledBackendClusterConnection *backend_end_point = GetCurrentPooledBackendClusterConnection();
    if (backend_end_point == NULL)
        return false;
    backend_end_point->pwd_size = pwd_size;
    memcpy(backend_end_point->password, password, pwd_size);
    backend_end_point->password[pwd_size] = 0; /* null terminate */
    backend_end_point->passwordType = passwordType;
    return true;
}

bool
SaveAuthKindForBackendConnection(int auth_kind)
{
    PooledBackendClusterConnection *backend_end_point = GetCurrentPooledBackendClusterConnection();
    if (backend_end_point == NULL)
        return false;
    backend_end_point->auth_kind = auth_kind;
    return true;
}

int
GetAuthKindForCurrentPoolBackendConnection(void)
{
    PooledBackendClusterConnection *backend_end_point = GetCurrentPooledBackendClusterConnection();
    if (backend_end_point == NULL)
        return -1;
    return backend_end_point->auth_kind;
}

bool ClearChildPooledConnectionData(void)
{
    PooledBackendClusterConnection *backend_end_point;

    if (processType != PT_CHILD)
        return false;

    backend_end_point = GetCurrentPooledBackendClusterConnection();

    if (backend_end_point == NULL)
    {
        ereport(LOG,
                (errmsg("cannot get backend connection for child process")));
        return false;
    }
    memset(backend_end_point, 0, sizeof(PooledBackendClusterConnection));
    return true;
}

/* Discard the backend connection.
 * If the connection is borrowed from the global pool
 * clean it up too
 */
bool DiscardCurrentBackendConnection(bool release_pool)
{
    BackendClusterConnection *current_backend_con;
    int i, pool_id;

    if (processType != PT_CHILD)
        return false;

    current_backend_con = GetBackendClusterConnection();

    if (release_pool)
        ReleaseClusterConnection(release_pool);

    pool_id = current_backend_con->pool_id;

    // if (release_pool && pool_id >= 0 && pool_id < pool_config->max_pool_size)
    //     ReleasePooledConnectionFromChild(parent_link, true);

    pool_send_frontend_exits(current_backend_con);

    for (i = 0; i < NUM_BACKENDS; i++)
    {
        if (current_backend_con->slots[i].con)
        {
            pool_close(current_backend_con->slots[i].con, true);
            current_backend_con->slots[i].con = NULL;
            current_backend_con->slots[i].closetime = time(NULL);
            current_backend_con->slots[i].state = CONNECTION_SLOT_EMPTY;
            current_backend_con->slots[i].key = -1;
            current_backend_con->slots[i].pid = -1;
        }
    }
    /* Free Startup Packet */
    if (current_backend_con->sp)
    {
        if (current_backend_con->sp->database)
            pfree(current_backend_con->sp->database);
        if (current_backend_con->sp->user)
            pfree(current_backend_con->sp->user);

        pfree(current_backend_con->sp->startup_packet);
        pfree(current_backend_con->sp);
        current_backend_con->sp = NULL;
    }
    if (!release_pool)
    {
        /* Restore the pool_id */
        current_backend_con->pool_id = pool_id;
    }
    return true;
}

/* Should already have the pool entry linked */
bool SetupNewConnectionIntoChild(StartupPacket *sp)
{
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    if (processType != PT_CHILD)
        return false;

    ereport(DEBUG1, (errmsg("SetupNewConnectionIntoChild pool_id:%d", current_backend_con->pool_id)));
    ImportStartupPacketIntoChild(sp, NULL);

    /* Slots should already have been initialized by Connect backend */
    return true;
}

void
ImportStartupPacketIntoChild(StartupPacket *sp, char *startup_packet_data)
{
    BackendClusterConnection *current_backend_con = GetBackendClusterConnection();

    if (sp->len <= 0 || sp->len >= MAX_STARTUP_PACKET_LENGTH)
        ereport(ERROR,
                (errmsg("incorrect packet length (%d)", sp->len)));

    current_backend_con->sp = MemoryContextAlloc(TopMemoryContext, sizeof(StartupPacket));
    current_backend_con->sp->len = sp->len;
    current_backend_con->sp->startup_packet = MemoryContextAlloc(TopMemoryContext, current_backend_con->sp->len);

    memcpy(current_backend_con->sp->startup_packet,
           startup_packet_data ? startup_packet_data : sp->startup_packet, current_backend_con->sp->len);

    current_backend_con->sp->major = sp->major;
    current_backend_con->sp->minor = sp->minor;

    current_backend_con->sp->database = pstrdup(sp->database);
    current_backend_con->sp->user = pstrdup(sp->user);

    if (sp->major == PROTO_MAJOR_V3 && sp->application_name)
    {
        /* adjust the application name pointer in new packet */
        current_backend_con->sp->application_name = current_backend_con->sp->startup_packet + (current_backend_con->sp->application_name - current_backend_con->sp->startup_packet);
    }
    else
        current_backend_con->sp->application_name = NULL;
}
/*
 * Create actual connections to backends.
 * New connection resides in TopMemoryContext.
 */
bool ConnectBackendClusterSockets(void)
{
    int active_backend_count = 0;
    int i;
    bool status_changed = false;
    volatile BACKEND_STATUS status;

    MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

    for (i = 0; i < NUM_BACKENDS; i++)
    {
        ereport(DEBUG1,
                (errmsg("creating new connection to backend"),
                 errdetail("connecting %d backend", i)));

        if (!VALID_BACKEND(i))
        {
            ereport(DEBUG1,
                    (errmsg("creating new connection to backend"),
                     errdetail("skipping backend slot %d because backend_status = %d",
                               i, BACKEND_INFO(i).backend_status)));
            continue;
        }

        /*
         * Make sure that the global backend status in the shared memory
         * agrees the local status checked by VALID_BACKEND. It is possible
         * that the local status is up, while the global status has been
         * changed to down by failover.
         */
        status = BACKEND_INFO(i).backend_status;
        if (status != CON_UP && status != CON_CONNECT_WAIT)
        {
            ereport(DEBUG1,
                    (errmsg("creating new connection to backend"),
                     errdetail("skipping backend slot %d because global backend_status = %d",
                               i, BACKEND_INFO(i).backend_status)));

            /* sync local status with global status */
            *(my_backend_status[i]) = status;
            continue;
        }

        if (ConnectBackendSocketForBackendCluster(i) == false)
        {
            /* set down status to local status area */
            *(my_backend_status[i]) = CON_DOWN;
            pool_get_my_process_info()->need_to_restart = 1; // TODO: check if this is needed
        }
        else
        {

            // p->info[i].client_idle_duration = 0;
            // p->slots[i] = s;

            pool_init_params(&clusterConnection.backend_end_point->params);

            if (BACKEND_INFO(i).backend_status != CON_UP)
            {
                BACKEND_INFO(i).backend_status = CON_UP;
                pool_set_backend_status_changed_time(i);
                status_changed = true;
            }
            active_backend_count++;
        }
    }

    if (status_changed)
        (void)write_status_file();

    MemoryContextSwitchTo(oldContext);

    if (active_backend_count > 0)
    {
        return true;
    }

    return false;
}

/*
 * Create socket connection to backend for a backend connection slot.
 */
bool
ConnectBackendSocketForBackendCluster(int slot_no)
{
    BackendNodeConnection *cp = &clusterConnection.slots[slot_no];
    BackendInfo *b = &pool_config->backend_desc->backend_info[slot_no];
    int fd;

    if (*b->backend_hostname == '/')
    {
        fd = connect_unix_domain_socket(slot_no, TRUE);
    }
    else
    {
        fd = connect_inet_domain_socket(slot_no, TRUE);
    }

    if (fd < 0)
    {
        cp->state = CONNECTION_SLOT_SOCKET_CONNECTION_ERROR;
        /*
         * If failover_on_backend_error is true, do failover. Otherwise,
         * just exit this session or skip next health node.
         */
        if (pool_config->failover_on_backend_error)
        {
            notice_backend_error(slot_no, REQ_DETAIL_SWITCHOVER);
            ereport(FATAL,
                    (errmsg("failed to create a backend connection"),
                     errdetail("executing failover on backend")));
        }
        else
        {
            /*
             * If we are in streaming replication mode and the node is a
             * standby node, then we skip this node to avoid fail over.
             */
            if (SL_MODE && !IS_PRIMARY_NODE_ID(slot_no))
            {
                ereport(LOG,
                        (errmsg("failed to create a backend %d connection", slot_no),
                         errdetail("skip this backend because because failover_on_backend_error is off and we are in streaming replication mode and node is standby node")));

                /* set down status to local status area */
                *(my_backend_status[slot_no]) = CON_DOWN;

                /* if main_node_id is not updated, then update it */
                if (Req_info->main_node_id == slot_no)
                {
                    int old_main = Req_info->main_node_id;
                    Req_info->main_node_id = get_next_main_node();
                    ereport(LOG,
                            (errmsg("main node %d is down. Update main node to %d",
                                    old_main, Req_info->main_node_id)));
                }
                return false;
            }
            else
            {
                ereport(FATAL,
                        (errmsg("failed to create a backend %d connection", slot_no),
                         errdetail("not executing failover because failover_on_backend_error is off")));
            }
        }
        return false;
    }

    cp->con = pool_open(fd, true);
    cp->key = -1;
    cp->pid = -1;
    cp->state = CONNECTION_SLOT_SOCKET_CONNECTION_ONLY;
    cp->con->pooled_backend_ref = &clusterConnection.backend_end_point->conn_slots[slot_no];
    cp->con->pooled_backend_ref->create_time = time(NULL);
    return true;
}

/*
 * check_socket_status() * RETURN : 0 = > OK
 * -1 = > broken socket.
 */

int check_socket_status(int fd)
{
    fd_set rfds;
    int result;
    struct timeval t;

    for (;;)
    {
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);

        t.tv_sec = t.tv_usec = 0;

        result = select(fd + 1, &rfds, NULL, NULL, &t);
        if (result < 0 && errno == EINTR)
        {
            continue;
        }
        else
        {
            return (result == 0 ? 0 : -1);
        }
    }

    return -1;
}