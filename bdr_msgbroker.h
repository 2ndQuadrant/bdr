#ifndef BDR_MSGBROKER_H
#define BDR_MSGBROKER_H

#include "datatype/timestamp.h"

struct WaitEventSet;
struct WaitEvent;

extern void msgb_service_connections(struct WaitEvent *occurred_events, int nevents,
									 long *max_next_wait_ms);

extern void msgb_startup(int max_connections, Size recv_queue_size);

extern void msgb_shutdown(void);

extern void msgb_shmem_init(int max_local_nodes);

extern void msgb_add_peer(uint32 peer_id, const char *dsn);

extern void msgb_remove_peer(uint32 peer_id);

extern void msgb_alter_peer(uint32 peer_id, const char *dsn);

#define MSGB_SCHEMA "bdr"

/* Broker-internal use only */
extern int msgb_max_peers;

#endif
