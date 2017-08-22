#ifndef BDR_MSGBROKER_SEND_H
#define BDR_MSGBROKER_SEND_H

struct WaitEventSet;
struct WaitEvent;

extern void msgb_startup_send(void);
extern void msgb_shutdown_send(void);

typedef enum MsgbSendStatus
{
	MSGB_MSGSTATUS_QUEUED,
	MSGB_MSGSTATUS_SENDING,
	MSGB_MSGSTATUS_DELIVERED,
	MSGB_MSGSTATUS_NOTFOUND
} MsgbSendStatus;

/*
 * This hook is invoked by the broker when the set of active sockets has
 * changed, and it needs to generate a new wait-event set with the
 * new sockets.
 *
 * This must (possibly after some delay) re-create the wait-event set with at
 * least msgb_get_wait_event_space_needed() entries then call
 *msgb_get_wait_event_space_needed msgb_wait_event_set_recreated with the new wait-set.
 *
 * Must be defined to use the broker.
 */
typedef void (*msgb_request_recreate_wait_event_set_hook_type)(WaitEventSet *old_set);

extern msgb_request_recreate_wait_event_set_hook_type msgb_request_recreate_wait_event_set_hook;

extern int msgb_get_wait_event_space_needed(void);

extern void msgb_wait_event_set_recreated(WaitEventSet *new_wait_set);

extern int msgb_queue_message(uint32 destination, const char * payload, Size payload_size);

extern MsgbSendStatus msgb_message_status(uint32 destination, int msgid);

extern void msgb_service_connections_send(WaitEvent *occurred_events, int nevents, long *max_next_wait_ms);

extern void msgb_add_send_peer(uint32 destination_id, const char *dsn);

extern void msgb_alter_send_peer(uint32 peer_id, const char *new_dsn);

extern void msgb_remove_send_peer(uint32 destination_id);

#endif
