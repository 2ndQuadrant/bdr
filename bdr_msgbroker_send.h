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

typedef struct WaitEventSet * (*msgb_recreate_wait_event_set_hook_type)(struct WaitEventSet *old_set, int max_entries);

extern msgb_recreate_wait_event_set_hook_type msgb_recreate_wait_event_set_hook;

extern void msgb_add_destination(uint32 destination_id, const char *dsn);

extern void msgb_remove_destination(uint32 destination_id);

extern int msgb_queue_message(uint32 destination, const char * payload, Size payload_size);

extern MsgbSendStatus msgb_message_status(uint32 destination, int msgid);

extern void msgb_service_connections_send(WaitEvent *occurred_events, int nevents);

extern void msgb_add_send_peer(uint32 destination_id, const char *dsn);

extern void msgb_remove_send_peer(uint32 destination_id);

#endif
