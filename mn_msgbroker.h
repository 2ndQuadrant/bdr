#ifndef BDR_MSGBROKER_H
#define BDR_MSGBROKER_H

#include "datatype/timestamp.h"

struct WaitEventSet;
struct WaitEvent;

/*
 * Fill the requested wait events callback.
 *
 * This is called by the request waitevents once it succeeded creating the
 * space for them.
 */
typedef void (*mn_waitevents_fill_cb)(WaitEventSet *new_set);

/*
 * WaitEvent space request.
 *
 * Note that this can be called multiple times during the lifetime of
 * consensus worker (mainly because of nodes being added/removed). So the
 * callback needs to be able to cope with that.
 *
 * Also note that this is request for total number of WaitEvents needed, *not*
 * diff to current state.
 */
typedef void (*mn_request_waitevents_fn)(int nwaitevents, mn_waitevents_fill_cb cb);

typedef void (*msgb_receive_cb)(uint32 origin, const char *payload, Size payload_size);

extern void msgb_wakeup(struct WaitEvent *occurred_events, int nevents,
						long *max_next_wait_ms);

extern void msgb_startup(uint32 local_node_id, mn_request_waitevents_fn request_waitevents,
						 msgb_receive_cb receive_cb, Size recv_queue_size);
extern void msgb_shutdown(void);

extern void msgb_add_peer(uint32 peer_id, const char *dsn);
extern void msgb_remove_peer(uint32 peer_id);

/* TODO */
#define MSGB_SCHEMA "bdr"

#endif
