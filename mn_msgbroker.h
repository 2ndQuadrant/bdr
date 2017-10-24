#ifndef BDR_MSGBROKER_H
#define BDR_MSGBROKER_H

#include "datatype/timestamp.h"

struct WaitEventSet;
struct WaitEvent;

typedef void (*msgb_receive_cb)(uint32 origin, const char *payload, Size payload_size);

extern void msgb_wakeup(struct WaitEvent *occurred_events, int nevents,
						long *max_next_wait_ms);

extern void msgb_startup(char *channel, uint32 local_node_id,
			 msgb_receive_cb receive_cb, Size recv_queue_size);
extern void msgb_shutdown(void);

extern void msgb_add_peer(uint32 peer_id, const char *dsn);
extern void msgb_remove_peer(uint32 peer_id);
extern void msgb_update_peer(uint32 peer_id, const char *dsn);

/* TODO */
#define MSGB_SCHEMA "bdr"

#endif
