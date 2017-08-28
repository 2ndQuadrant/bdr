#ifndef BDR_SHMEM_H
#define BDR_SHMEM_H

#include "storage/shm_mq.h"

#define BDR_SHMEM_MAGIC 0x51ae3d34 

#define SHM_MQ_SIZE 1024

struct PGPROC;
struct LWLock;

/*
 * A static shmem entry for a BDR manager of a local node.
 *
 * TODO: In future this will mainly be used to find the DSM segment for the
 * manager.  But for now we're just statically allocating some memory
 * as a quick hack for minimal functionality.
 */
typedef struct BdrManagerShmem
{
	uint32		node_id;
	struct PGPROC *manager;

	/* TODO HACK HACK HACK
	 *
	 * We should be doing something more like a set of shmem ToCs with these
	 * queues in them. This is just a minimal hack so we can submit/recieve
	 * with the manager to get minimal functionality woking. TODO
	 *
	 * If these queues are busy when another worker tries to do something
	 * it'll just have to back off and try again. Or error.
	 *
	 * These are really shm_mq not char, of course. (See "hack")
	 *
	 * recv and send are from manager's PoV
	 *
	 * Take bdr_ctx->lock in shared mode before attaching to these. This guards
	 * against them being overwritten (after another peer detaches) during an
	 * attach attempt. TODO: finer grained locking?
	 */
	char		shm_recv_mq[SHM_MQ_SIZE];
	char		shm_send_mq[SHM_MQ_SIZE];
} BdrManagerShmem;

typedef struct BdrShmemContext
{
	struct LWLock *lock;
	/*
	 * TODO: per-manager DSM segment pointers where we can store our shmem
	 * memory queues to talk to the manager
	 */
	uint8			max_local_nodes;
	BdrManagerShmem managers[FLEXIBLE_ARRAY_MEMBER];
} BdrShmemContext;

extern BdrShmemContext *bdr_ctx;

extern void bdr_shmem_init(int max_local_nodes);

extern BdrManagerShmem* bdr_shmem_lookup_manager_segment(uint32 node_id, bool missing_ok);

extern BdrManagerShmem* bdr_shmem_allocate_manager_segment(uint32 node_id);

extern void bdr_shmem_release_manager_segment(BdrManagerShmem *seg);

#endif
