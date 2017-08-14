#ifndef BDR_MSGBROKER_RECEIVE_H
#define BDR_MSGBROKER_RECEIVE_H

extern void msgb_startup_receive(Size recv_queue_size);
extern void msgb_shutdown_receive(void);

extern void msgb_shmem_init_receive(int max_local_nodes);

typedef void (*msgb_received_hook_type)(uint32 origin, const char *payload, Size payload_size);

extern msgb_received_hook_type msgb_received_hook;

extern void msgb_service_connections_receive(void);

extern void msgb_add_receive_peer(uint32 origin_id);
extern void msgb_remove_receive_peer(uint32 origin_id);

#endif
