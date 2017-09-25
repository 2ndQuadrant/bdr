#ifndef BDR_MSGBROKER_RECEIVE_H
#define BDR_MSGBROKER_RECEIVE_H

#include "mn_msgbroker.h"

extern void msgb_startup_receive(uint32 local_node_id, Size recv_queue_size,
								 msgb_receive_cb receive_cb);
extern void msgb_shutdown_receive(void);

extern void msgb_wakeup_receive(void);

#endif
