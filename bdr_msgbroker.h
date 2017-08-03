#ifndef BDR_MSGBROKER_H
#define BDR_MSGBROKER_H

#include "datatype/timestamp.h"

struct WaitEventSet;
struct WaitEvent;

typedef void (*msgb_received_hook_type)(uint32 origin, const char *payload, uint32 payload_size);

extern msgb_received_hook_type msgb_received_hook;

typedef struct WaitEventSet * (*msgb_recreate_wait_event_set_hook_type)(struct WaitEventSet *old_set, int max_entries);

extern msgb_recreate_wait_event_set_hook_type msgb_recreate_wait_event_set_hook;

extern void msgb_set_wait_event_set(struct WaitEventSet* set);

extern void msgb_service_connections(struct WaitEvent *occurred_events, int nevents);

extern void msgb_add_destination(uint32 destionation_id, const char *dsn);

extern void msgb_remove_destination(uint32 destination_id);

extern int msgb_queue_message(uint32 destination, const char * payload, uint32 payload_size);

extern int msgb_message_status(int msgid);

extern void msgb_startup(int max_connections);

extern void msgb_shutdown(void);

#endif
