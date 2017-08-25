/*-------------------------------------------------------------------------
 *
 * bdr_functions.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_functions.c
 *
 * SQL-callable function interface for BDR
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "utils/builtins.h"

#include "bdr_catcache.h"
#include "bdr_messaging.h"
#include "bdr_functions.h"

PG_FUNCTION_INFO_V1(bdr_decode_message_payload);

Datum
bdr_decode_message_payload(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}


PG_FUNCTION_INFO_V1(bdr_decode_state);

Datum
bdr_decode_state(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unimplemented")));
}

PG_FUNCTION_INFO_V1(bdr_submit_message);

/*
 * Test function to submit a no-op message into the consensus
 * messaging system for replay to peer nodes.
 */
Datum
bdr_submit_message(PG_FUNCTION_ARGS)
{
	BdrMessage *msg;
	const char *dummy_payload = text_to_cstring(PG_GETARG_TEXT_P(0));
	Size dummy_payload_length;
	uint64 handle;
	char handle_str[33];

	if (!bdr_is_active_db())
		elog(ERROR, "BDR is not active in this database");

	dummy_payload_length = strlen(dummy_payload)+ 1;

	msg = palloc(offsetof(BdrMessage,payload) + dummy_payload_length);
	msg->message_type = BDR_MSG_NOOP;
	msg->payload_length = dummy_payload_length;
	memcpy(msg->payload, dummy_payload, dummy_payload_length);

	handle = bdr_msgs_enqueue_one(msg);
	if (handle == 0)
		elog(WARNING, "manager couldn't enqueue message, try again later");
	else
		elog(WARNING, "manager enqueued message with handle "UINT64_FORMAT, handle);

	snprintf(&handle_str[0], 33, UINT64_FORMAT, handle);
	PG_RETURN_TEXT_P(cstring_to_text(handle_str));
}
