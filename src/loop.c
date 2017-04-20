/*
Copyright (c) 2009-2014 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

Contributors:
   Roger Light - initial implementation and documentation.

*/

#define _GNU_SOURCE

#include <config.h>

#include <assert.h>
#ifndef WIN32
#include <sys/epoll.h>
#else
#include <process.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#ifndef WIN32
#  include <sys/socket.h>
#endif
#include <time.h>

#ifdef WITH_WEBSOCKETS
#  include <libwebsockets.h>
#endif

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <send_mosq.h>
#include <time_mosq.h>
#include <util_mosq.h>

extern bool flag_reload;
#ifdef WITH_PERSISTENCE
extern bool flag_db_backup;
#endif
extern bool flag_tree_print;
extern int run;
#ifdef WITH_SYS_TREE
extern int g_clients_expired;
#endif

void epoll_loop_handle_errors(struct mosquitto_db *db, struct epoll_event *epollfd);
void epoll_loop_handle_reads_writes(struct mosquitto_db *db, struct epoll_event *epollfd);
 int epoll_is_listenersock(int sock, int *listensock, int listensock_count);
 int epoll_add(struct mosquitto_db *db, int fd, uint32_t events);

int mosquitto_main_loop(struct mosquitto_db *db, int *listensock, int listensock_count, int listener_max)
{
#ifdef WITH_SYS_TREE
    time_t start_time = mosquitto_time();
#endif
#ifdef WITH_PERSISTENCE
    time_t last_backup = mosquitto_time();
#endif
    time_t now = 0;
    time_t now_time;
    int time_count;
    struct mosquitto *context, *ctxt_tmp;
#ifndef WIN32
    sigset_t sigblock, origsig;
#endif
    int i, fd, nfds;
    struct epoll_event *epollfds = NULL;
    int epollfd_count = 0;
#ifdef WITH_BRIDGE
    int bridge_sock;
    int rc;
#endif
    int context_count;
    time_t expiration_check_time = 0;

#ifndef WIN32
    sigemptyset(&sigblock);
    sigaddset(&sigblock, SIGINT);
#endif

    db->epollfd = epoll_create1(0);

    if (db->config->persistent_client_expiration > 0)
    {
        expiration_check_time = time(NULL) + 3600;
    }

    for (i = 0; i < listensock_count; i++)
    {
        epoll_add(db, listensock[i], EPOLLIN);
    }

    while (run)
    {
        mosquitto__free_disused_contexts(db);
#ifdef WITH_SYS_TREE
        if (db->config->sys_interval > 0)
        {
            mqtt3_db_sys_update(db, db->config->sys_interval, start_time);
        }
#endif

        context_count = HASH_CNT(hh_sock, db->contexts_by_sock);
#ifdef WITH_BRIDGE
        context_count += db->bridge_count;
#endif

        if (listensock_count + context_count > epollfd_count || !epollfds)
        {
            epollfd_count = listensock_count + context_count;
            epollfds = _mosquitto_realloc(epollfds, sizeof(struct epoll_event) * epollfd_count);
            if (!epollfds)
            {
                _mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
                return MOSQ_ERR_NOMEM;
            }
        }

        memset(epollfds, -1, sizeof(struct epoll_event) * epollfd_count);

        now_time = time(NULL);

        time_count = 0;

        HASH_ITER(hh_sock, db->contexts_by_sock, context, ctxt_tmp)
        {
            if (time_count > 0)
            {
                time_count--;
            }
            else
            {
                time_count = 1000;
                now = mosquitto_time();
            }

            if (context->sock != INVALID_SOCKET)
            {
#ifdef WITH_BRIDGE
                if (context->bridge)
                {
                    _mosquitto_check_keepalive(db, context);
                    if (context->bridge->round_robin == false
                            && context->bridge->cur_address != 0
                            && now > context->bridge->primary_retry)
                    {

                        if (_mosquitto_try_connect(context, context->bridge->addresses[0].address, context->bridge->addresses[0].port, &bridge_sock, NULL, false) == MOSQ_ERR_SUCCESS)
                        {
                            COMPAT_CLOSE(bridge_sock);
                            _mosquitto_socket_close(db, context);
                            context->bridge->cur_address = context->bridge->address_count - 1;
                        }
                    }
                }
#endif

                /* Local bridges never time out in this fashion. */
                if (!(context->keepalive)
                        || context->bridge
                        || now - context->last_msg_in < (time_t)(context->keepalive) * 3 / 2)
                {
                    if (mqtt3_db_message_write(db, context) == MOSQ_ERR_SUCCESS)
                    {
                        // pollfds[pollfd_index].fd = context->sock;
                        // pollfds[pollfd_index].events = POLLIN;
                        // pollfds[pollfd_index].revents = 0;
                        // if (context->current_out_packet || context->state == mosq_cs_connect_pending)
                        // {
                        //     pollfds[pollfd_index].events |= POLLOUT;
                        // }
                        // context->pollfd_index = pollfd_index;
                        // pollfd_index++;
                    }
                    else
                    {
                        do_disconnect(db, context);
                    }
                }
                else
                {
                    if (db->config->connection_messages == true)
                    {
                        _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s has exceeded timeout, disconnecting.", context->id);
                    }
                    /* Client has exceeded keepalive*1.5 */
                    do_disconnect(db, context);
                }
            }
        }

#ifdef WITH_BRIDGE

        time_count = 0;

        for (i = 0; i < db->bridge_count; i++)
        {
            if (!db->bridges[i]) continue;

            context = db->bridges[i];

            if (context->sock == INVALID_SOCKET)
            {
                if (time_count > 0)
                {
                    time_count--;
                }
                else
                {
                    time_count = 1000;
                    now = mosquitto_time();
                }
                /* Want to try to restart the bridge connection */
                if (!context->bridge->restart_t)
                {
                    context->bridge->restart_t = now + context->bridge->restart_timeout;
                    context->bridge->cur_address++;
                    if (context->bridge->cur_address == context->bridge->address_count)
                    {
                        context->bridge->cur_address = 0;
                    }
                    if (context->bridge->round_robin == false && context->bridge->cur_address != 0)
                    {
                        context->bridge->primary_retry = now + 5;
                    }
                }
                else
                {
                    if (context->bridge->start_type == bst_lazy && context->bridge->lazy_reconnect)
                    {
                        rc = mqtt3_bridge_connect(db, context);
                        if (rc)
                        {
                            context->bridge->cur_address++;
                            if (context->bridge->cur_address == context->bridge->address_count)
                            {
                                context->bridge->cur_address = 0;
                            }
                        }
                    }
                    if (context->bridge->start_type == bst_automatic && now > context->bridge->restart_t)
                    {
                        context->bridge->restart_t = 0;
                        rc = mqtt3_bridge_connect(db, context);
                        if (rc == MOSQ_ERR_SUCCESS)
                        {
                            // pollfds[pollfd_index].fd = context->sock;
                            // pollfds[pollfd_index].events = POLLIN;
                            // pollfds[pollfd_index].revents = 0;
                            // if (context->current_out_packet)
                            // {
                            //     pollfds[pollfd_index].events |= POLLOUT;
                            // }
                            // context->pollfd_index = pollfd_index;
                            // pollfd_index++;
                        }
                        else
                        {
                            /* Retry later. */
                            context->bridge->restart_t = now + context->bridge->restart_timeout;

                            context->bridge->cur_address++;
                            if (context->bridge->cur_address == context->bridge->address_count)
                            {
                                context->bridge->cur_address = 0;
                            }
                        }
                    }
                }
            }
        }
#endif
        now_time = time(NULL);

        if (db->config->persistent_client_expiration > 0 && now_time > expiration_check_time)
        {
            HASH_ITER(hh_id, db->contexts_by_id, context, ctxt_tmp)
            {
                if (context->sock == -1 && context->clean_session == 0)
                {
                    /* This is a persistent client, check to see if the
                     * last time it connected was longer than
                     * persistent_client_expiration seconds ago. If so,
                     * expire it and clean up.
                     */
                    if (now_time > context->disconnect_t + db->config->persistent_client_expiration)
                    {
                        _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Expiring persistent client %s due to timeout.", context->id);
#ifdef WITH_SYS_TREE
                        g_clients_expired++;
#endif
                        context->clean_session = true;
                        context->state = mosq_cs_expiring;
                        do_disconnect(db, context);
                    }
                }
            }
            expiration_check_time = time(NULL) + 3600;
        }

        mqtt3_db_message_timeout_check(db, db->config->retry_interval);

#ifndef WIN32
        sigprocmask(SIG_SETMASK, &sigblock, &origsig);
        nfds = epoll_wait(db->epollfd, epollfds, epollfd_count, 100);
        sigprocmask(SIG_SETMASK, &origsig, NULL);
#endif

        for (i = 0; i < nfds; i++)
        {
            if ((epollfds[i].events & EPOLLERR) || (epollfds[i].events & EPOLLHUP) || (!(epollfds[i].events & EPOLLIN)))
            {
                epoll_loop_handle_errors(db, &epollfds[i]);
            }

            else if (epoll_is_listenersock(epollfds[i].data.fd, listensock, listensock_count))
            {
                if (epollfds[i].events & (EPOLLIN | EPOLLPRI))
                {
                    while ((fd = mqtt3_socket_accept(db, epollfds[i].data.fd)) != -1)
                    {
                        epoll_add(db, fd, EPOLLIN);
                    }
                }
            }

            else
            {
                epoll_loop_handle_reads_writes(db, &epollfds[i]);
            }
        }
#ifdef WITH_PERSISTENCE
        if (db->config->persistence && db->config->autosave_interval)
        {
            if (db->config->autosave_on_changes)
            {
                if (db->persistence_changes > db->config->autosave_interval)
                {
                    mqtt3_db_backup(db, false);
                    db->persistence_changes = 0;
                }
            }
            else
            {
                if (last_backup + db->config->autosave_interval < mosquitto_time())
                {
                    mqtt3_db_backup(db, false);
                    last_backup = mosquitto_time();
                }
            }
        }
#endif

#ifdef WITH_PERSISTENCE
        if (flag_db_backup)
        {
            mqtt3_db_backup(db, false);
            flag_db_backup = false;
        }
#endif
        if (flag_reload)
        {
            _mosquitto_log_printf(NULL, MOSQ_LOG_INFO, "Reloading config.");
            mqtt3_config_read(db->config, true);
            mosquitto_security_cleanup(db, true);
            mosquitto_security_init(db, true);
            mosquitto_security_apply(db);
            mqtt3_log_init(db->config->log_type, db->config->log_dest, db->config->log_facility);
            flag_reload = false;
        }
        if (flag_tree_print)
        {
            mqtt3_sub_tree_print(&db->subs, 0);
            flag_tree_print = false;
        }
#ifdef WITH_WEBSOCKETS
        for (i = 0; i < db->config->listener_count; i++)
        {
            /* Extremely hacky, should be using the lws provided external poll
             * interface, but their interface has changed recently and ours
             * will soon, so for now websockets clients are second class
             * citizens. */
            if (db->config->listeners[i].ws_context)
            {
                libwebsocket_service(db->config->listeners[i].ws_context, 0);
            }
        }
#endif
    }

    if (epollfds) _mosquitto_free(epollfds);
    return MOSQ_ERR_SUCCESS;
}

int epoll_add(struct mosquitto_db *db, int fd, uint32_t events)
{
    struct epoll_event epollfd;
    epollfd.data.fd = fd;
    epollfd.events = events;

    if (epoll_ctl(db->epollfd, EPOLL_CTL_ADD, fd, &epollfd) < 0)
    {
        return 0;
    }

    return 1;
}

int epoll_is_listenersock(int sock, int *listensock, int listensock_count)
{
    int i;

    for (i = 0; i < listensock_count; ++i)
    {
        if (sock == listensock[i])
        {
            return 1;
        }
    }
    return 0;
}

void do_disconnect(struct mosquitto_db *db, struct mosquitto *context)
{
    if (context->state == mosq_cs_disconnected)
    {
        return;
    }
#ifdef WITH_WEBSOCKETS
    if (context->wsi)
    {
        if (context->state != mosq_cs_disconnecting)
        {
            context->state = mosq_cs_disconnect_ws;
        }
        if (context->wsi)
        {
            libwebsocket_callback_on_writable(context->ws_context, context->wsi);
        }
        context->sock = INVALID_SOCKET;
    }
    else
#endif
    {
        if (db->config->connection_messages == true)
        {
            if (context->state != mosq_cs_disconnecting)
            {
                _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Socket error on client %s, disconnecting.", context->id);
            }
            else
            {
                _mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE, "Client %s disconnected.", context->id);
            }
        }
        mqtt3_context_disconnect(db, context);
#ifdef WITH_BRIDGE
        if (context->clean_session && !context->bridge)
        {
#else
        if (context->clean_session)
        {
#endif
            mosquitto__add_context_to_disused(db, context);
            if (context->id)
            {
                HASH_DELETE(hh_id, db->contexts_by_id, context);
                _mosquitto_free(context->id);
                context->id = NULL;
            }
        }
        context->state = mosq_cs_disconnected;
    }
}

/* Error ocurred, probably an fd has been closed.
 * Loop through and check them all.
 */
void epoll_loop_handle_errors(struct mosquitto_db *db, struct epoll_event *epollfd)
{
    struct mosquitto *context;

    HASH_FIND(hh_sock, db->contexts_by_sock, &epollfd->data.fd, sizeof(int), context);

    if (context != NULL)
    {
        do_disconnect(db, context);
    }
}

void epoll_loop_handle_reads_writes(struct mosquitto_db *db, struct epoll_event *epollfd)
{
    struct mosquitto *context;
#ifdef WIN32
    char err;
#else
    int err;
#endif
    socklen_t len;

    HASH_FIND(hh_sock, db->contexts_by_sock, &epollfd->data.fd, sizeof(int), context);

    if (context != NULL)
    {
        assert(epollfd->data.fd == context->sock);
#ifdef WITH_TLS
        if (epollfd->events & EPOLLIN ||
                context->want_write ||
                (context->ssl && context->state == mosq_cs_new))
        {
#else
        if (epollfd->events & EPOLLIN)
        {
#endif
            if (context->state == mosq_cs_connect_pending)
            {
                len = sizeof(int);
                if (!getsockopt(context->sock, SOL_SOCKET, SO_ERROR, &err, &len))
                {
                    if (err == 0)
                    {
                        context->state = mosq_cs_new;
                    }
                }
                else
                {
                    do_disconnect(db, context);
                    return;
                }
            }
            if (_mosquitto_packet_write(context))
            {
                do_disconnect(db, context);
                return;
            }
        }
#ifdef WITH_TLS
        if (epollfd->events & EPOLLIN ||
                (context->ssl && context->state == mosq_cs_new))
        {
#else
        if (epollfd->events & EPOLLIN)
        {
#endif
            if (_mosquitto_packet_read(db, context))
            {
                do_disconnect(db, context);
                return;
            }
        }
    }
}

