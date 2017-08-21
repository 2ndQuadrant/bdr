#ifndef BDR_SHMEM_H
#define BDR_SHMEM_H

#include "storage/shm_mq.h"

#include "pglogical_worker.h"

#define BDR_SHMEM_MAGIC 0x51ae3d34 

/*
 * Needed for in_shm_mq, to work around
 * https://www.postgresql.org/message-id/CAMsr+YHmm=01LsuEYR6YdZ8CLGfNK_fgdgi+QXUjF+JeLPvZQg@mail.gmail.com
 * per comments on pglogical's
 * handle_sigterm(...)
 */
inline static shm_mq_handle *
bdr_shm_mq_attach(shm_mq *mq, dsm_segment *seg, BackgroundWorkerHandle *handle)
{
	shm_mq_handle *ret;
	in_shm_mq = true;
	ret = shm_mq_attach(mq, seg, handle);
	in_shm_mq = false;
	return ret;
}

inline static shm_mq_result
bdr_shm_mq_receive(shm_mq_handle *mqh, Size *nbytesp, void **datap, bool nowait)
{
	shm_mq_result ret;
	in_shm_mq = true;
	ret = shm_mq_receive(mqh, nbytesp, datap, nowait);
	in_shm_mq = false;
	return ret;
	
}

inline static shm_mq_result
bdr_shm_mq_send(shm_mq_handle *mqh, Size nbytes, const void *data, bool nowait)
{
	shm_mq_result ret;
	in_shm_mq = true;
	ret = shm_mq_send(mqh, nbytes, data, nowait);
	in_shm_mq = false;
	return ret;
}

inline static shm_mq_result
bdr_shm_mq_sendv(shm_mq_handle *mqh, shm_mq_iovec *iov, int iovcnt, bool nowait)
{
	shm_mq_result ret;
	in_shm_mq = true;
	ret = shm_mq_sendv(mqh, iov, iovcnt, nowait);
	in_shm_mq = false;
	return ret;
}

inline static shm_mq_result
bdr_shm_mq_wait_for_attach(shm_mq_handle *mqh)
{
	shm_mq_result ret;
	in_shm_mq = true;
	ret = shm_mq_wait_for_attach(mqh);
	in_shm_mq = false;
	return ret;
}

#endif
