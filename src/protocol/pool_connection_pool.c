/* -*-pgsql-c-*- */
/*
 * $Header$
 *
 * pgpool: a language independent connection pool server for PostgreSQL
 * written by Tatsuo Ishii
 *
 * Copyright (c) 2003-2023	PgPool Global Development Group
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
 * pool_connection_pool.c: connection pool stuff
 */
#include "config.h"

#include <sys/types.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <netdb.h>
#include <time.h>
#include <stdio.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "pool.h"
#include "main/pgpool_ipc.h"
#include "context/pool_query_context.h"
#include "utils/pool_stream.h"
#include "utils/palloc.h"
#include "pool_config.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "protocol/pool_connection_pool.h"
#include "protocol/pool_process_query.h"
#include "protocol/pool_pg_utils.h"
#include "main/pool_internal_comms.h"


#include "context/pool_process_context.h"

int parent_link = -1;
POOL_CONNECTION_POOL *pool_connection_pool;
static int	pool_index;			/* Active pool index */
ChildBackendConnection	child_backend_connection;


volatile sig_atomic_t backend_timer_expired = 0;	/* flag for connection
													 * closed timer is expired */
volatile sig_atomic_t health_check_timer_expired;	/* non 0 if health check
													 * timer expired */
static bool ConnectBackendSlotSocket(int slot_no);
static int	check_socket_status(int fd);
static bool connect_with_timeout(int fd, struct addrinfo *walk, char *host, int port, bool retry);

#define TMINTMAX 0x7fffffff

ChildBackendConnection* 
GetChildBackendConnection(void)
{
	return &child_backend_connection;
}
/*
* initialize connection pools. this should be called once at the startup.
*/
void
pool_init_cp(int parent_link_fd)
{
	child_backend_connection.backend_end_point = NULL;
	child_backend_connection.borrowed = false;
	child_backend_connection.pool_id = -1;
	memset(child_backend_connection.slots, 0, sizeof(ChildBackendConnectionSlot) * MAX_NUM_BACKENDS);
	parent_link = parent_link_fd;
}

/*
* Get connection by user and database
*/
POOL_CONNECTION_POOL *
pool_get_cp(char *user, char *database, int protoMajor, int check_socket)
{
	pool_sigset_t oldmask;
	// int		count;
	// int		sockets[MAX_NUM_BACKENDS];
	// POOL_IPC_RETURN_CODES	ipc_ret;
    // ProcessInfo *pro_info = pool_get_my_process_info();

	int			i,
				freed = 0;
	ConnectionInfo *info;

	POOL_SETMASK2(&BlockSig, &oldmask);
	// ipc_ret = BorrowBackendConnection(parent_link, database, user, protoMajor, 0, &count, sockets);
	// if (ipc_ret == SOCKETS_RECEIVED)
	// {
	// 	/* TODO Handle broken sockets */
	// 	ereport(LOG,
	// 			(errmsg("received %d sockets from parent pool_id:%d", count, pro_info->pool_id),
	// 			 errdetail("database:%s user:%s protoMajor:%d", database, user, protoMajor)));
	// 	child_backend_connection.backend_end_point = GetBackendEndPoint(pro_info->pool_id);
	// 	child_backend_connection.borrowed = true;
	// 	child_backend_connection.pool_id = pro_info->pool_id;
	// 	for (i = 0; i < count; i++)
	// 	{
	// 		int slot_no = pro_info->backend_ids[i];
	// 		child_backend_connection.slots[slot_no]->connection = pool_open(sockets[i], true);
	// 		// p->info[i].create_time = time(NULL);
	// 		// p->info[i].client_idle_duration = 0;
	// 		// p->slots[i] = s;
	// 		pool_init_params(&child_backend_connection.slots[slot_no]->connection->params);
	// 		// Copy the parameters from the global pool entry
	// 	}
	// }
	// else if (ipc_ret == EMPTY_POOL_SLOT_RESERVED)
	// {
	// 	ereport(LOG,
	// 			(errmsg("received EMPTY_POOL_SLOT_RESERVED from parent pool_id:%d", pro_info->pool_id),
	// 			 errdetail("database:%s user:%s protoMajor:%d", database, user, protoMajor)));
	// 	child_backend_connection.backend_end_point = GetBackendEndPoint(pro_info->pool_id);;
	// 	child_backend_connection.borrowed = false;
	// 	child_backend_connection.pool_id = pro_info->pool_id;
	// 	/* Now we need to connect to backend*/
	// }
	// else if (ipc_ret == NON_POOL_CONNECTION)
	// {
	// 	ereport(LOG,
	// 			(errmsg("received NON_POOL_CONNECTION from parent pool_id:%d", pro_info->pool_id),
	// 			 errdetail("database:%s user:%s protoMajor:%d", database, user, protoMajor)));
	// 	child_backend_connection.backend_end_point = NULL;
	// 	child_backend_connection.borrowed = false;
	// 	child_backend_connection.pool_id = -1;
	// }

	POOL_SETMASK(&oldmask);
	return NULL;
}

/*
 * disconnect and release a connection to the database
 */
void
pool_discard_cp(char *user, char *database, int protoMajor)
{
#ifdef NOT_USED
	POOL_CONNECTION_POOL *p = pool_get_cp(user, database, protoMajor, 0);
	ConnectionInfo *info;
	int			i,
				freed = 0;

	if (p == NULL)
	{
		ereport(LOG,
				(errmsg("cannot get connection pool for user: \"%s\" database: \"%s\", while discarding connection pool", user, database)));
		return;
	}

	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (!VALID_BACKEND(i))
			continue;

		if (!freed)
		{
			pool_free_startup_packet(CONNECTION_SLOT(p, i)->sp);
			freed = 1;
		}
		CONNECTION_SLOT(p, i)->sp = NULL;
		pool_close(CONNECTION(p, i));
		pfree(CONNECTION_SLOT(p, i));
	}

	info = p->info;
	memset(p, 0, sizeof(POOL_CONNECTION_POOL));
	p->info = info;
	memset(p->info, 0, sizeof(ConnectionInfo) * MAX_NUM_BACKENDS);
#endif
}

#ifdef NOT_USED
/*
* create a connection pool by user and database
*/
POOL_CONNECTION_POOL *
pool_create_cp(void)
{
	int			i,
				freed = 0;
	time_t		closetime;
	POOL_CONNECTION_POOL *oldestp;
	POOL_CONNECTION_POOL *ret;
	ConnectionInfo *info;
	int		main_node_id;

	POOL_CONNECTION_POOL *p = pool_connection_pool;

	/* if no connection pool exists we have no reason to live */
	if (p == NULL)
		ereport(ERROR,
				(return_code(2),
				 errmsg("unable to create connection"),
				 errdetail("connection pool is not initialized")));

	for (i = 0; i < pool_config->max_pool; i++)
	{
		if (in_use_backend_id(p) < 0)	/* is this connection pool out of use? */
		{
			ret = new_connection(p);
			if (ret)
				pool_index = i;
			return ret;
		}
		p++;
	}
	ereport(DEBUG1,
			(errmsg("creating connection pool"),
			 errdetail("no empty connection slot was found")));

	/*
	 * no empty connection slot was found. look for the oldest connection and
	 * discard it.
	 */
	oldestp = p = pool_connection_pool;
	closetime = TMINTMAX;
	pool_index = 0;

	for (i = 0; i < pool_config->max_pool; i++)
	{
		main_node_id = in_use_backend_id(p);
		if (main_node_id < 0)
			elog(ERROR, "no in use backend found");	/* this should not happen */

		ereport(DEBUG1,
				(errmsg("creating connection pool"),
				 errdetail("user: %s database: %s closetime: %ld",
						   CONNECTION_SLOT(p, main_node_id)->sp->user,
						   CONNECTION_SLOT(p, main_node_id)->sp->database,
						   CONNECTION_SLOT(p, main_node_id)->closetime)));

		if (CONNECTION_SLOT(p, main_node_id)->closetime < closetime)
		{
			closetime = CONNECTION_SLOT(p, main_node_id)->closetime;
			oldestp = p;
			pool_index = i;
		}
		p++;
	}

	p = oldestp;
	main_node_id = in_use_backend_id(p);
	if (main_node_id < 0)
		elog(ERROR, "no in use backend found");	/* this should not happen */
	pool_send_frontend_exits(p);

	ereport(DEBUG1,
			(errmsg("creating connection pool"),
			 errdetail("discarding old %zd th connection. user: %s database: %s",
					   oldestp - pool_connection_pool,
					   CONNECTION_SLOT(p, main_node_id)->sp->user,
					   CONNECTION_SLOT(p, main_node_id)->sp->database)));

	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (CONNECTION_SLOT(p, i) == NULL)
			continue;

		if (!freed)
		{
			pool_free_startup_packet(CONNECTION_SLOT(p, i)->sp);
			CONNECTION_SLOT(p, i)->sp = NULL;

			freed = 1;
		}

		pool_close(CONNECTION(p, i));
		pfree(CONNECTION_SLOT(p, i));
	}

	info = p->info;
	memset(p, 0, sizeof(POOL_CONNECTION_POOL));
	p->info = info;
	memset(p->info, 0, sizeof(ConnectionInfo) * MAX_NUM_BACKENDS);

	ret = new_connection(p);
	return ret;
}

#endif

/*
 * set backend connection close timer
 */
void
pool_connection_pool_timer(POOL_CONNECTION_POOL * backend)
{
	#ifdef NOT_USED
	POOL_CONNECTION_POOL *p = pool_connection_pool;
	int			i;

	ereport(DEBUG1,
			(errmsg("setting backend connection close timer"),
			 errdetail("close time %ld", time(NULL))));

	/* Set connection close time */
	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (CONNECTION_SLOT(backend, i))
			CONNECTION_SLOT(backend, i)->closetime = time(NULL);
	}

	if (pool_config->connection_life_time == 0)
		return;

	/* look for any other timeout */
	for (i = 0; i < pool_config->max_pool; i++, p++)
	{
		if (!MAIN_CONNECTION(p))
			continue;
		if (!MAIN_CONNECTION(p)->sp)
			continue;
		if (MAIN_CONNECTION(p)->sp->user == NULL)
			continue;

		if (p != backend && MAIN_CONNECTION(p)->closetime)
			return;
	}

	/* no other timer found. set my timer */
	ereport(DEBUG1,
			(errmsg("setting backend connection close timer"),
			 errdetail("setting alarm after %d seconds", pool_config->connection_life_time)));

	pool_alarm(pool_backend_timer_handler, pool_config->connection_life_time);
	#endif
}

/*
 * backend connection close timer handler
 */
RETSIGTYPE
pool_backend_timer_handler(int sig)
{
	backend_timer_expired = 1;
}

void
pool_backend_timer(void)
{
#define TMINTMAX 0x7fffffff
#ifdef UN_USED
	POOL_CONNECTION_POOL *p = pool_connection_pool;
	int			i,
				j;
	time_t		now;
	time_t		nearest = TMINTMAX;
	ConnectionInfo *info;

	POOL_SETMASK(&BlockSig);

	now = time(NULL);

	ereport(DEBUG1,
			(errmsg("backend timer handler called at %ld", now)));

	for (i = 0; i < pool_config->max_pool; i++, p++)
	{
		if (!MAIN_CONNECTION(p))
			continue;
		if (!MAIN_CONNECTION(p)->sp)
			continue;
		if (MAIN_CONNECTION(p)->sp->user == NULL)
			continue;

		/* timer expire? */
		if (MAIN_CONNECTION(p)->closetime)
		{
			int			freed = 0;

			ereport(DEBUG1,
					(errmsg("backend timer handler called"),
					 errdetail("expire time: %ld",
							   MAIN_CONNECTION(p)->closetime + pool_config->connection_life_time)));

			if (now >= (MAIN_CONNECTION(p)->closetime + pool_config->connection_life_time))
			{
				/* discard expired connection */
				ereport(DEBUG1,
						(errmsg("backend timer handler called"),
						 errdetail("expired user: \"%s\" database: \"%s\"",
								   MAIN_CONNECTION(p)->sp->user, MAIN_CONNECTION(p)->sp->database)));
				pool_send_frontend_exits(p);

				for (j = 0; j < NUM_BACKENDS; j++)
				{
					if (!VALID_BACKEND(j))
						continue;

					if (!freed)
					{
						pool_free_startup_packet(CONNECTION_SLOT(p, j)->sp);
						freed = 1;
					}
					CONNECTION_SLOT(p, j)->sp = NULL;
					pool_close(CONNECTION(p, j));
					pfree(CONNECTION_SLOT(p, j));
				}
				info = p->info;
				memset(p, 0, sizeof(POOL_CONNECTION_POOL));
				p->info = info;
				memset(p->info, 0, sizeof(ConnectionInfo) * MAX_NUM_BACKENDS);
			}
			else
			{
				/* look for nearest timer */
				if (MAIN_CONNECTION(p)->closetime < nearest)
					nearest = MAIN_CONNECTION(p)->closetime;
			}
		}
	}

	/* any remaining timer */
	if (nearest != TMINTMAX)
	{
		nearest = pool_config->connection_life_time - (now - nearest);
		if (nearest <= 0)
			nearest = 1;
		pool_alarm(pool_backend_timer_handler, nearest);
	}
	update_pooled_connection_count();
	POOL_SETMASK(&UnBlockSig);
#endif
}

/*
 * connect to postmaster through INET domain socket
 */
int
connect_inet_domain_socket(int slot, bool retry)
{
	char	   *host;
	int			port;

	host = pool_config->backend_desc->backend_info[slot].backend_hostname;
	port = pool_config->backend_desc->backend_info[slot].backend_port;

	return connect_inet_domain_socket_by_port(host, port, retry);
}

/*
 * connect to postmaster through UNIX domain socket
 */
int
connect_unix_domain_socket(int slot, bool retry)
{
	int			port;
	char	   *socket_dir;

	port = pool_config->backend_desc->backend_info[slot].backend_port;
	socket_dir = pool_config->backend_desc->backend_info[slot].backend_hostname;

	return connect_unix_domain_socket_by_port(port, socket_dir, retry);
}

/*
 * Connect to PostgreSQL server by using UNIX domain socket.
 * If retry is true, retry to call connect() upon receiving EINTR error.
 */
int
connect_unix_domain_socket_by_port(int port, char *socket_dir, bool retry)
{
	struct sockaddr_un addr;
	int			fd;
	int			len;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd == -1)
	{
		ereport(LOG,
				(errmsg("failed to connect to PostgreSQL server by unix domain socket"),
				 errdetail("create socket failed with error \"%m\"")));
		return -1;
	}

	memset((char *) &addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	snprintf(addr.sun_path, sizeof(addr.sun_path), "%s/.s.PGSQL.%d", socket_dir, port);
	len = sizeof(struct sockaddr_un);

	for (;;)
	{
		if (exit_request)		/* exit request already sent */
		{
			ereport(LOG,
					(errmsg("failed to connect to PostgreSQL server by unix domain socket"),
					 errdetail("exit request has been sent")));
			close(fd);
			return -1;
		}

		if (connect(fd, (struct sockaddr *) &addr, len) < 0)
		{
			if ((errno == EINTR && retry) || errno == EAGAIN)
				continue;
			close(fd);
			ereport(LOG,
					(errmsg("failed to connect to PostgreSQL server by unix domain socket"),
					 errdetail("connect to \"%s\" failed with error \"%m\"", addr.sun_path)));

			return -1;
		}
		break;
	}

	return fd;
}

/*
 * Connect to backend using pool_config->connect_timeout.
 *
 * fd: the socket
 * walk: backend address to connect
 * host and port: backend hostname and port number. Only for error message
 * purpose.
 * retry: true if need to retry
 */
static bool
connect_with_timeout(int fd, struct addrinfo *walk, char *host, int port, bool retry)
{
	struct timeval *tm;
	struct timeval timeout;
	fd_set		rset,
				wset;
	int			sts;
	int			error;
	socklen_t	socklen;

	socket_set_nonblock(fd);

	for (;;)
	{
		if (exit_request)		/* exit request already sent */
		{
			ereport(LOG,
					(errmsg("failed to connect to PostgreSQL server on \"%s:%d\" using INET socket", host, port),
					 errdetail("exit request has been sent")));
			close(fd);
			return false;
		}

		if (health_check_timer_expired) /* has health check timer expired */
		{
			ereport(LOG,
					(errmsg("failed to connect to PostgreSQL server on \"%s:%d\" using INET socket", host, port),
					 errdetail("health check timer expired")));
			close(fd);
			return false;
		}

		if (connect(fd, walk->ai_addr, walk->ai_addrlen) < 0)
		{
			if (errno == EISCONN)
			{
				/* Socket is already connected */
				break;
			}

			if ((errno == EINTR && retry) || errno == EAGAIN)
				continue;

			/*
			 * If error was "connect(2) is in progress", then wait for
			 * completion.  Otherwise error out.
			 */
			if (errno != EINPROGRESS && errno != EALREADY)
			{
				ereport(LOG,
						(errmsg("failed to connect to PostgreSQL server on \"%s:%d\"", host, port),
						 errdetail("%m")));
				return false;
			}

			if (pool_config->connect_timeout == 0)
				tm = NULL;
			else
			{
				tm = &timeout;
				timeout.tv_sec = pool_config->connect_timeout / 1000;
				if (timeout.tv_sec == 0)
				{
					timeout.tv_usec = pool_config->connect_timeout * 1000;
				}
				else
				{
					timeout.tv_usec = (pool_config->connect_timeout - timeout.tv_sec * 1000) * 1000;
				}
			}

			FD_ZERO(&rset);
			FD_SET(fd, &rset);
			FD_ZERO(&wset);
			FD_SET(fd, &wset);
			sts = select(fd + 1, &rset, &wset, NULL, tm);

			if (sts == 0)
			{
				/* select timeout */
				if (retry)
				{
					ereport(LOG,
							(errmsg("trying connecting to PostgreSQL server on \"%s:%d\" by INET socket", host, port),
							 errdetail("timed out. retrying...")));
					continue;
				}
				else
				{
					ereport(LOG,
							(errmsg("failed to connect to PostgreSQL server on \"%s:%d\", timed out", host, port)));
					return false;
				}
			}
			else if (sts > 0)
			{
				/*
				 * If read data or write data was set, either connect
				 * succeeded or error.  We need to figure it out. This is the
				 * hardest part in using non blocking connect(2).  See W.
				 * Richard Stevens's "UNIX Network Programming: Volume 1,
				 * Second Edition" section 15.4.
				 */
				if (FD_ISSET(fd, &rset) || FD_ISSET(fd, &wset))
				{
					error = 0;
					socklen = sizeof(error);
					if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &socklen) < 0)
					{
						/* Solaris returns error in this case */
						ereport(LOG,
								(errmsg("failed to connect to PostgreSQL server on \"%s:%d\", getsockopt() failed", host, port),
								 errdetail("%m")));

						return false;
					}

					/* Non Solaris case */
					if (error != 0)
					{
						ereport(LOG,
								(errmsg("failed to connect to PostgreSQL server on \"%s:%d\", getsockopt() failed", host, port),
								 errdetail("%m")));
						return false;
					}
				}
				else
				{
					ereport(LOG,
							(errmsg("failed to connect to PostgreSQL server on \"%s:%d\", both read data and write data was not set", host, port)));

					return false;
				}
			}
			else				/* select returns error */
			{
				if ((errno == EINTR && retry) || errno == EAGAIN)
				{
					ereport(LOG,
							(errmsg("trying to connect to PostgreSQL server on \"%s:%d\" using INET socket", host, port),
							 errdetail("select() interrupted. retrying...")));
					continue;
				}

				/*
				 * select(2) was interrupted by certain signal and we guess it
				 * was not SIGALRM because health_check_timer_expired was not
				 * set (if the variable was set, we can assume that SIGALRM
				 * handler was called). Surely this is not a health check time
				 * out. We can assume that this is a transient case. So we
				 * will retry again...
				 */
				if (health_check_timer_expired == 0 && errno == EINTR)
				{
					ereport(LOG,
							(errmsg("connect_inet_domain_socket: select() interrupted by certain signal. retrying...")));
					continue;
				}

				else if (health_check_timer_expired && errno == EINTR)
				{
					ereport(LOG,
							(errmsg("failed to connect to PostgreSQL server on \"%s:%d\" using INET socket", host, port),
							 errdetail("health check timer expired")));
				}
				else
				{
					ereport(LOG,
							(errmsg("failed to connect to PostgreSQL server on \"%s:%d\" using INET socket", host, port),
							 errdetail("select() system call failed with error \"%m\"")));
				}
				close(fd);
				return false;
			}
		}
		break;
	}

	socket_unset_nonblock(fd);
	return true;
}

/*
 * Connect to PostgreSQL server by using INET domain socket.
 * If retry is true, retry to call connect() upon receiving EINTR error.
 */
int
connect_inet_domain_socket_by_port(char *host, int port, bool retry)
{
	int			fd = -1;
	int			on = 1;
	char	   *portstr;
	int			ret;
	struct addrinfo *res;
	struct addrinfo *walk;
	struct addrinfo hints;

	/*
	 * getaddrinfo() requires a string because it also accepts service names,
	 * such as "http".
	 */
	if (asprintf(&portstr, "%d", port) == -1)
	{
		ereport(WARNING,
				(errmsg("failed to connect to PostgreSQL server, asprintf() failed"),
				 errdetail("%m")));

		return -1;
	}

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = PF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((ret = getaddrinfo(host, portstr, &hints, &res)) != 0)
	{
		ereport(WARNING,
				(errmsg("failed to connect to PostgreSQL server, getaddrinfo() failed with error \"%s\"", gai_strerror(ret))));

		free(portstr);
		return -1;
	}

	free(portstr);

	for (walk = res; walk != NULL; walk = walk->ai_next)
	{
		fd = socket(walk->ai_family, walk->ai_socktype, walk->ai_protocol);
		if (fd < 0)
		{
			ereport(WARNING,
					(errmsg("failed to connect to PostgreSQL server, socket() failed"),
					 errdetail("%m")));
			continue;
		}

		/* set nodelay */
		if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
					   (char *) &on,
					   sizeof(on)) < 0)
		{
			ereport(WARNING,
					(errmsg("failed to connect to PostgreSQL server, setsockopt() failed"),
					 errdetail("%m")));

			close(fd);
			freeaddrinfo(res);
			return -1;
		}

		if (!connect_with_timeout(fd, walk, host, port, retry))
		{
			close(fd);
			continue;
		}

		freeaddrinfo(res);
		return fd;
	}

	freeaddrinfo(res);
	return -1;
}

/*
 * Create socket connection to backend for a backend connection slot.
 */
static bool ConnectBackendSlotSocket(int slot_no)
{
	ChildBackendConnectionSlot *cp = &child_backend_connection.slots[slot_no];
	BackendInfo *b = &pool_config->backend_desc->backend_info[slot_no];
	int			fd;

	if (*b->backend_hostname == '/')
	{
		fd = connect_unix_domain_socket(slot_no, TRUE);
	}
	else
	{
		fd = connect_inet_domain_socket(slot_no, TRUE);
	}

	if (fd < 0)
		return false;

	cp->con = pool_open(fd, true);
	cp->key = -1;
	cp->pid = -1;
	cp->con->pooled_backend_ref = &child_backend_connection.backend_end_point->conn_slots[slot_no];
	return true;
}

/*
 * Create actual connections to backends.
 * New connection resides in TopMemoryContext.
 */
bool
ConnectBackendSocktes(void)
{
	int			active_backend_count = 0;
	int			i;
	bool		status_changed = false;
	volatile BACKEND_STATUS	status;

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

		if (ConnectBackendSlotSocket(i) == false)
		{
			/*
			 * If failover_on_backend_error is true, do failover. Otherwise,
			 * just exit this session or skip next health node.
			 */
			if (pool_config->failover_on_backend_error)
			{
				notice_backend_error(i, REQ_DETAIL_SWITCHOVER);
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
				if (SL_MODE && !IS_PRIMARY_NODE_ID(i))
				{
					ereport(LOG,
							(errmsg("failed to create a backend %d connection", i),
							 errdetail("skip this backend because because failover_on_backend_error is off and we are in streaming replication mode and node is standby node")));

					/* set down status to local status area */
					*(my_backend_status[i]) = CON_DOWN;

					/* if main_node_id is not updated, then update it */
					if (Req_info->main_node_id == i)
					{
						int			old_main = Req_info->main_node_id;

						Req_info->main_node_id = get_next_main_node();

						ereport(LOG,
								(errmsg("main node %d is down. Update main node to %d",
										old_main, Req_info->main_node_id)));
					}

					/*
					 * make sure that we need to restart the process after
					 * finishing this session
					 */
					pool_get_my_process_info()->need_to_restart = 1;
					continue;
				}
				else
				{
					ereport(FATAL,
							(errmsg("failed to create a backend %d connection", i),
							 errdetail("not executing failover because failover_on_backend_error is off")));
				}
			}
			child_exit(POOL_EXIT_AND_RESTART);
		}
		else
		{
			// p->info[i].create_time = time(NULL);
			// p->info[i].client_idle_duration = 0;
			// p->slots[i] = s;

			pool_init_params(&child_backend_connection.slots[i].con->params);

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
		(void) write_status_file();

	MemoryContextSwitchTo(oldContext);

	if (active_backend_count > 0)
	{
		return true;
	}

	return false;
}

/* check_socket_status()
 * RETURN: 0 => OK
 *        -1 => broken socket.
 */
static int
check_socket_status(int fd)
{
	fd_set		rfds;
	int			result;
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

/*
 * Return current used index (i.e. frontend connected)
 */
int
pool_pool_index(void)
{
	return pool_index;
}

/*
 * send frontend exiting messages to all connections.  this is called
 * in any case when child process exits, for example failover, child
 * life time expires or child max connections expires.
 */

void
close_all_backend_connections(void)
{
#ifdef NOT_USED
	int			i;
	POOL_CONNECTION_POOL *p = pool_connection_pool;

	pool_sigset_t oldmask;

	POOL_SETMASK2(&BlockSig, &oldmask);

	for (i = 0; i < pool_config->max_pool; i++, p++)
	{
		if (!MAIN_CONNECTION(p))
			continue;
		if (!MAIN_CONNECTION(p)->sp)
			continue;
		if (MAIN_CONNECTION(p)->sp->user == NULL)
			continue;
		pool_send_frontend_exits(p);
	}

	POOL_SETMASK(&oldmask);
#endif
}

void update_pooled_connection_count(void)
{
	int i;
	int count = 0;
	POOL_CONNECTION_POOL *p = pool_connection_pool;
	for (i = 0; i < pool_config->max_pool; i++)
	{
		if (&MAIN_CONNECTION(p))
			count++;
	}
	pool_get_my_process_info()->pooled_connections = count;
}

/*
 * Return the first node id in use.
 * If no node is in use, return -1.
 */
int
in_use_backend_id(POOL_CONNECTION_POOL *pool)
{
	int	i;

	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (pool->slots[i])
			return i;
	}

	return -1;
}

/* Global Connection Pool owned by Main process and lives in shared memory */
ConnectionPoolEntry	*ConnectionPool = NULL; /* Global connection pool */
static int get_sockets_array(BackendEndPoint*  backend_endpoint, int **sockets, int* num_sockets, bool pooled_socks);
static void import_startup_packet_into_child(StartupPacket* sp, char* startup_packet_data);
static void import_pooled_startup_packet_into_child(BackendEndPoint* backend_end_point);
static void register_new_lease(int pool_id, LEASE_TYPES	lease_type, IPC_Endpoint* ipc_endpoint);


size_t
get_global_connection_pool_shared_mem_size(void)
{
	return sizeof(ConnectionPoolEntry) * pool_config->max_pool_size;
}

void init_global_connection_pool(void)
{
	memset(ConnectionPool, 0, get_global_connection_pool_shared_mem_size());
}

BackendEndPoint*
GetBackendEndPoint(int pool_id)
{
	if (pool_id < 0 || pool_id >= pool_config->max_pool_size)
		return NULL;
	return &ConnectionPool[pool_id].endPoint;
}

ConnectionPoolEntry*
GetConnectionPoolEntry(int pool_id)
{
	if (pool_id < 0 || pool_id >= pool_config->max_pool_size)
		return NULL;
	return &ConnectionPool[pool_id];
}

bool
ExportLocalSocketsToBackendPool(void)
{
	int *sockets = NULL;
	int num_sockets = 0;

	if (processType != PT_CHILD)
		return false;

	get_sockets_array( GetChildBackendConnection()->backend_end_point, &sockets, &num_sockets, false);
	if (sockets && num_sockets > 0)
	{
		bool ret;
		ret = SendBackendSocktesToMainPool(parent_link, num_sockets, sockets);
		pfree(sockets);
		return ret;
	}
	else
		ereport(LOG,
			(errmsg("No socket found to transfer to to global connection pool")));

	return true;
}

bool
InstallSocketsInConnectionPool(int pool_id, int *sockets)
{
	int i;
	ConnectionPoolEntry *pool_entry;

	if (processType != PT_MAIN)
		return false;

	pool_entry = GetConnectionPoolEntry(pool_id);
	if (!pool_entry)
		return false;

	for (i = 0; i < pool_entry->endPoint.num_sockets; i++)
	{
		int slot_no = pool_entry->endPoint.backend_ids[i];
		ConnectionPool[pool_id].endPoint.conn_slots[slot_no].socket = sockets[i];
	}
	return true;
}

bool
ExportLocalBackendConnectionToPool(void)
{
	ChildBackendConnection* current_backend_con = GetChildBackendConnection();
	StartupPacket *sp = current_backend_con->sp;
	int pool_id = current_backend_con->pool_id;
	int i, sock_index;
	BackendEndPoint* backend_end_point = GetBackendEndPoint(pool_id);

	if (backend_end_point == NULL)
		return false;

	/* verify the length first */
	if (sp->len <= 0 || sp->len >= MAX_STARTUP_PACKET_LENGTH)
	{	ereport(ERROR,
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

	backend_end_point->sp.database= backend_end_point->database;
	backend_end_point->sp.user = backend_end_point->user;

	if (sp->major == PROTO_MAJOR_V3 && sp->application_name)
	{
		/* adjust the application name pointer in new packet */
		backend_end_point->sp.application_name = backend_end_point->sp.startup_packet + (sp->application_name - sp->startup_packet);
	}
	else
		backend_end_point->sp.application_name = NULL;

	// memset(backend_end_point->conn_slots, 0, sizeof(BackendConnection) * MAX_NUM_BACKENDS);

	sock_index = 0;
	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (VALID_BACKEND(i))
		{
			backend_end_point->conn_slots[i].auth_kind = current_backend_con->slots[i].con->auth_kind;
			backend_end_point->conn_slots[i].pwd_size = current_backend_con->slots[i].con->pwd_size;
			memcpy(backend_end_point->conn_slots[i].password,current_backend_con->slots[i].con->password, sizeof(backend_end_point->conn_slots[i].password));
			backend_end_point->conn_slots[i].passwordType = current_backend_con->slots[i].con->passwordType;
			memcpy(backend_end_point->conn_slots[i].salt,current_backend_con->slots[i].con->salt, sizeof(backend_end_point->conn_slots[i].salt));

			// backend_end_point->conn_slots[i].key = current_backend_con->slots[i].key;
			// backend_end_point->conn_slots[i].pid = current_backend_con->slots[i].pid;
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
	return true;
}

bool
DiscardPooledConnectio(int pool_id)
{
	int i;
	BackendEndPoint* backend_end_point = GetBackendEndPoint(pool_id);

	if (backend_end_point == NULL)
	{
		ereport(LOG,
			(errmsg("cannot get backend connection for pool_id:%d", pool_id)));
		return false;
	}

	for (i = 0; i < backend_end_point->num_sockets; i++)
	{
		int slot_no = backend_end_point->backend_ids[i];
		close(backend_end_point->conn_slots[slot_no].socket);
	}

	memset(backend_end_point, 0, sizeof(BackendEndPoint));
	return true;
}

bool
DiscardChildLocalBackendConnection(bool close_socket)
{
	ChildBackendConnection* current_backend_con = GetChildBackendConnection();
	int i;

	if (processType != PT_CHILD)
		return false;

	int pool_id = current_backend_con->pool_id;
	for (i = 0; i < NUM_BACKENDS; i++)
	{
		if (current_backend_con->slots[i].con)
		{
			pool_close(current_backend_con->slots[i].con, close_socket);
			current_backend_con->slots[i].con = NULL;
		}
	}
	/* Free Startup Packet */
	if (current_backend_con->sp)
	{
		if (current_backend_con->sp->database)
			pfree(current_backend_con->sp->database);
		if(current_backend_con->sp->user)
			pfree(current_backend_con->sp->user);

		pfree(current_backend_con->sp->startup_packet);
		pfree(current_backend_con->sp);
		current_backend_con->sp = NULL;
	}
	memset(current_backend_con, 0, sizeof(ChildBackendConnection));
	/* Restore the pool_id */
	current_backend_con->pool_id = pool_id;

	return true;
}

bool
InitializeChildLocalBackendConnection(int pool_id, StartupPacket* sp)
{
	int i;
	ChildBackendConnection* current_backend_con = GetChildBackendConnection();
	BackendEndPoint* backend_end_point = NULL;
	int num_sockets = backend_end_point->num_sockets;
	int *backend_ids = backend_end_point->backend_ids;

	if (processType != PT_CHILD)
		return false;
	
	if (pool_id > 0)
		backend_end_point = GetBackendEndPoint(pool_id);

	current_backend_con->pool_id = pool_id;
	current_backend_con->backend_end_point = backend_end_point;
	current_backend_con->borrowed = false;

	import_startup_packet_into_child(sp, NULL);

	/* Slots should already have been initialized by Connect backend */
	return true;
}

bool
ImportPoolConnectionIntoChild(int pool_id, int *sockets)
{
	int i;
	ChildBackendConnection* current_backend_con = GetChildBackendConnection();
	BackendEndPoint* backend_end_point = GetBackendEndPoint(pool_id);
	int num_sockets = backend_end_point->num_sockets;
	int *backend_ids = backend_end_point->backend_ids;

	if (backend_end_point == NULL)
		return false;

	current_backend_con->pool_id = pool_id;
	current_backend_con->backend_end_point = backend_end_point;
	current_backend_con->borrowed = true;

	import_pooled_startup_packet_into_child(backend_end_point);

	for (i = 0; i < num_sockets; i++)
	{
		int slot_no = backend_ids[i];

		current_backend_con->slots[slot_no].con = pool_open(sockets[i], true);
		current_backend_con->slots[slot_no].con->auth_kind = backend_end_point->conn_slots[slot_no].auth_kind;
		current_backend_con->slots[slot_no].con->pwd_size = backend_end_point->conn_slots[slot_no].pwd_size;
		current_backend_con->slots[slot_no].con->pooled_backend_ref = &backend_end_point->conn_slots[slot_no];
		
		memcpy(current_backend_con->slots[slot_no].con->password, backend_end_point->conn_slots[slot_no].password, sizeof(backend_end_point->conn_slots[slot_no].password));
		current_backend_con->slots[slot_no].con->passwordType =backend_end_point->conn_slots[slot_no].passwordType;
		memcpy(current_backend_con->slots[slot_no].con->salt, backend_end_point->conn_slots[slot_no].salt, sizeof(backend_end_point->conn_slots[slot_no].salt));

		current_backend_con->slots[slot_no].key = backend_end_point->conn_slots[slot_no].key;
		current_backend_con->slots[slot_no].pid = backend_end_point->conn_slots[slot_no].pid;


	}
	return true;
}

static void
import_pooled_startup_packet_into_child(BackendEndPoint* backend_end_point)
{

	ChildBackendConnection* current_backend_con = GetChildBackendConnection();

	if (backend_end_point->sp.len <= 0 || backend_end_point->sp.len >= MAX_STARTUP_PACKET_LENGTH)
		ereport(ERROR,
				(errmsg("incorrect packet length (%d)", backend_end_point->sp.len)));

	import_startup_packet_into_child(&backend_end_point->sp, backend_end_point->startup_packet_data);
}

static void
import_startup_packet_into_child(StartupPacket* sp, char* startup_packet_data)
{

	ChildBackendConnection* current_backend_con = GetChildBackendConnection();

	if (sp->len <= 0 || sp->len >= MAX_STARTUP_PACKET_LENGTH)
		ereport(ERROR,
				(errmsg("incorrect packet length (%d)", sp->len)));

	current_backend_con->sp = MemoryContextAlloc(TopMemoryContext, sizeof(StartupPacket));
	current_backend_con->sp->len = sp->len;
	current_backend_con->sp->startup_packet =  MemoryContextAlloc(TopMemoryContext, current_backend_con->sp->len); 

	memcpy(current_backend_con->sp->startup_packet,
		startup_packet_data?startup_packet_data: sp->startup_packet, current_backend_con->sp->len);

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

int
GetPooledConnectionForLending(char *user, char *database, int protoMajor, LEASE_TYPES *lease_type)
{
	int i;
	int free_pool_slot = -1;
	int unused_pool_slot = -1;
	ereport(LOG,
		(errmsg("Finding for user:%s database:%s protoMajor:%d", user, database, protoMajor)));
	for (i =0; i < pool_config->max_pool_size; i++)
	{
		if (ConnectionPool[i].status == POOL_ENTRY_READY &&
			ConnectionPool[i].borrower_pid <= 0 &&
			ConnectionPool[i].endPoint.sp.major == protoMajor )
		{
			if (strcmp(ConnectionPool[i].endPoint.user, user) == 0 &&
            	strcmp(ConnectionPool[i].endPoint.database, database) == 0)
				{
					*lease_type = LEASE_TYPE_READY_TO_USE;
					return i;
				}
		}
		else if (free_pool_slot == -1 && ConnectionPool[i].status == POOL_ENTRY_EMPTY)
			free_pool_slot = i;

	}
	ereport(LOG,
		(errmsg("No pooled connection for user:%s database:%s protoMajor:%d, Free slot:%d", user, database, protoMajor,free_pool_slot)));

	/* No pooled connection found. Return empty slot */
	if (free_pool_slot > -1)
	{
		*lease_type = LEASE_TYPE_EMPTY_SLOT_RESERVED;
		return free_pool_slot;
	}

	/* TODO Find the pooled entry that can be discarded and re-used */
	ereport(LOG,
		(errmsg("No pooled connection for user:%s database:%s protoMajor:%d, And No Free slot available ", user, database, protoMajor)));

	/* Nothing found */
	*lease_type = LEASE_TYPE_NO_AVAILABLE_SLOT;
	return -1;
}

bool
LeasePooledConnectionToChild(IPC_Endpoint* ipc_endpoint)
{
	ProcessInfo*	child_proc_info;

	LEASE_TYPES		lease_type;
	int				pool_id = -1;
	int				i;

	if (processType != PT_MAIN)
		return false;

	if (ipc_endpoint->proc_info_id < 0 || ipc_endpoint->proc_info_id >= pool_config->num_init_children)
		return false;
	child_proc_info = &process_info[ipc_endpoint->proc_info_id];

	if (child_proc_info->pid != ipc_endpoint->child_pid)
		return false;

	pool_id = GetPooledConnectionForLending(child_proc_info->user,
											child_proc_info->database,
											child_proc_info->major,
											&lease_type);

	ereport(LOG,
		(errmsg("pool_id:%d with lease type:%d reserved for child:%d", pool_id, lease_type, ipc_endpoint->child_pid)));

	if (InformChildAboutLeaseStatus(ipc_endpoint->child_link, lease_type) == false)
	{
		ereport(LOG,
			(errmsg("Failed to send (pool_id:%d) lease type:%d to child:%d", pool_id, lease_type, ipc_endpoint->child_pid)));
		return false;
	}

	register_new_lease(pool_id, lease_type, ipc_endpoint);

	if (lease_type == LEASE_TYPE_READY_TO_USE || lease_type == LEASE_TYPE_DISCART_AND_CREATE)
	{
		int *sockets = NULL;
		int num_sockets = 0;
		get_sockets_array(&ConnectionPool[pool_id].endPoint, &sockets, &num_sockets, true);
		if (sockets && num_sockets > 0)
		{
			bool ret;
			ret = TransferSocketsBetweenProcesses(ipc_endpoint->child_link, num_sockets, sockets);
			pfree(sockets);
			return ret;
		}
		else
			ereport(LOG,
				(errmsg("No socket found to transfer to to child:%d", ipc_endpoint->child_pid)));
	}
	return true;
}

bool
ReleasePooledConnection(ConnectionPoolEntry* pool_entry, IPC_Endpoint* ipc_endpoint, bool need_cleanup)
{
    if (pool_entry->borrower_pid != ipc_endpoint->child_pid)
    {
        ereport(LOG,
                (errmsg("child:%d is not the borrower of pool_id:%d borrowed by:%d", ipc_endpoint->child_pid, pool_entry->pool_id, pool_entry->borrower_pid)));
        return false;
    }
    pool_entry->borrower_pid = -1;
	pool_entry->need_cleanup = need_cleanup;
	pool_entry->status = POOL_ENTRY_READY;

	return true;
}

static void
register_new_lease(int pool_id, LEASE_TYPES lease_type, IPC_Endpoint* ipc_endpoint)
{
	ConnectionPool[pool_id].borrower_pid = ipc_endpoint->child_pid;
	if (lease_type == LEASE_TYPE_READY_TO_USE)
		ConnectionPool[pool_id].status = POOL_ENTRY_LEASED;
	else if (lease_type == LEASE_TYPE_EMPTY_SLOT_RESERVED)
		ConnectionPool[pool_id].status = POOL_ENTRY_RESERVED;
	ConnectionPool[pool_id].leased_count++;
	ConnectionPool[pool_id].leased_time = time(NULL);
}


/* TODO: Handle disconnected socktes */
static int
get_sockets_array(BackendEndPoint*  backend_endpoint, int **sockets, int* num_sockets, bool pooled_socks)
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
			ChildBackendConnection* current_backend_con = GetChildBackendConnection();
			socks[i] = 	current_backend_con->slots[sock_index].con->fd;
		}
	}

	*sockets = socks;
	return *num_sockets;
}

/*
 * locate and return the shared memory BackendConnection having the
 * backend connection with the pid
 * If the connection is found the *backend_node_id contains the backend node id
 * of the backend node that has the connection
 */
BackendConnection *
GetBackendConnectionByForBackendPID(int backend_pid, int *backend_node_id)
{
	int i;

	for (i =0; i < pool_config->max_pool_size; i++)
	{
		int con_slot;
		if (ConnectionPool[i].status == POOL_ENTRY_EMPTY ||
			ConnectionPool[i].endPoint.num_sockets <= 0)
			continue;
		for(con_slot = 0; con_slot < ConnectionPool[i].endPoint.num_sockets; con_slot++)
		{
			if (ConnectionPool[i].endPoint.conn_slots[con_slot].pid == backend_pid)
			{
				*backend_node_id = i;
				return &ConnectionPool[i].endPoint.conn_slots[con_slot];
			}
		}
	}
	return NULL;
}

BackendEndPoint*
GetBackendEndPointForCancelPacket(CancelPacket* cp)
{
	int i, con_slot;

	for (i =0; i < pool_config->max_pool_size; i++)
	{
		int con_slot;
		if (ConnectionPool[i].status == POOL_ENTRY_EMPTY ||
			ConnectionPool[i].endPoint.num_sockets <= 0)
			continue;
		for(con_slot = 0; con_slot < ConnectionPool[i].endPoint.num_sockets; con_slot++)
		{
			BackendConnection* c = &ConnectionPool[i].endPoint.conn_slots[con_slot];
			ereport(DEBUG2,
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