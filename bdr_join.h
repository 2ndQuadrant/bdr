#ifndef BDR_JOIN_H
#define BDR_JOIN_H

#include "access/xlogdefs.h"

#include "bdr_catalogs.h"

struct pg_conn;
typedef struct pg_conn PGconn;

struct BdrMessage;
enum BdrNodeState;
struct WaitEvent;
struct WaitEventSet;

/*
 * Extra data for BDR_NODE_STATE_JOIN_START
 */
typedef struct ExtraDataJoinStart
{
	const char * group_name;
} ExtraDataJoinStart;

/*
 * State journal extradata for BDR_NODE_STATE_JOIN_WAIT_CONFIRM
 * and BDR_NODE_STATE_WAIT_GLOBAL_SEQ_ID
 */
typedef struct ExtraDataConsensusWait
{
	uint64 request_message_handle;
} ExtraDataConsensusWait;

/*
 * State journal extradata fro BDR_NODE_STATE_JOIN_WAIT_CATCHUP
 */
typedef struct ExtraDataJoinWaitCatchup
{
	XLogRecPtr	min_catchup_lsn;
} ExtraDataJoinWaitCatchup;

/*
 * State journal extradata for BDR_NODE_STATE_JOIN_FAILED
 */
typedef struct ExtraDataJoinFailure
{
	const char * reason;
} ExtraDataJoinFailure;

extern BdrNodeInfo * get_remote_node_info(PGconn *conn);

extern PGconn *bdr_join_connect_remote(BdrNodeInfo *local,
	const char * remote_node_dsn);

extern void bdr_finish_connect_remote(PGconn *conn);

extern void bdr_join_handle_join_proposal(struct BdrMessage *msg);

extern void bdr_join_handle_standby_proposal(struct BdrMessage *msg);

extern void bdr_join_handle_active_proposal(struct BdrMessage *msg);

extern BdrNodeInfo *get_remote_node_info(PGconn *conn);

extern void bdr_join_copy_remote_nodegroup(BdrNodeInfo *local,
	BdrNodeInfo *remote);

extern void bdr_join_copy_remote_node(BdrNodeInfo *local,
	BdrNodeInfo *remote);

extern void bdr_join_continue(enum BdrNodeState cur_state,
	long *max_next_wait_msecs);

extern void bdr_gen_slot_name(Name slot_name, const char *dbname,
	const char *nodegroup, const char *provider_node,
	const char *subscriber_node);

/*
 * Integration into wait events in manager
 */

extern int bdr_join_get_wait_event_space_needed(void);

extern void bdr_join_wait_event(struct WaitEvent *events, int nevents, long *max_next_wait_ms);

extern void bdr_join_wait_event_set_recreated(struct WaitEventSet *new_set);


#endif
