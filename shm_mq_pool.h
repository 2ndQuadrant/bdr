#ifndef SHM_MQ_POOL_H
#define SHM_MQ_POOL_H

#include "storage/shm_mq.h"

struct MQPool;
typedef struct MQPool MQPool;

struct MQPoolConn;
typedef struct MQPoolConn MQPoolConn;

struct MQPoolerContext;
typedef struct MQPoolerContext MQPoolerContext;

extern MQPoolerContext *MQPoolerCtx;

typedef void (*shm_mq_pool_connect_cb) (MQPoolConn *mqconn);
typedef void (*shm_mq_pool_disconnect_cb) (MQPoolConn *mqconn);
typedef void (*shm_mq_pool_message_cb) (MQPoolConn *mqconn, void *data, Size len);

extern void shm_mq_pooler_shmem_init(void);

/* Pooling server interfaces. */
extern MQPool *shm_mq_pooler_new_pool(const char *name, int max_connections, Size recv_queue_size,
					   shm_mq_pool_connect_cb connect_cb,
					   shm_mq_pool_disconnect_cb disconnect_cb,
					   shm_mq_pool_message_cb message_cb);
extern void shm_mq_pooler_work(void);

/* Pooling cient interfaces. */
extern MQPoolConn *shm_mq_pool_get_connection(MQPool *mqpool, bool nowait);
extern void shm_mq_pool_disconnect(MQPoolConn *mqconn);
extern MQPool *shm_mq_pool_get_pool(const char *name);
extern bool shm_mq_pool_write(MQPoolConn *mqconn, StringInfo msg);
extern bool shm_mq_pool_receive(MQPoolConn *mqconn, StringInfo output, bool nowait);

#endif		/* SHM_MQ_POOL_H */
