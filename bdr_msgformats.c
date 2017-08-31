/*-------------------------------------------------------------------------
 *
 * bdr_msgformats.c
 * 		pglogical plugin for multi-master replication
 *
 * Copyright (c) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  bdr_msgformats.c
 *
 * Functions to serialize and deserialize messages (WAL messages,
 * consensus proposals, etc).
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"

#include "libpq/pqformat.h"

#include "bdr_msgformats.h"

void
wrapInStringInfo(StringInfo si, char *data, Size length)
{
	si->data = data;
	si->len = length;
	si->maxlen = -1;
	si->cursor = -1;
}

void
msg_serialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request)
{
	initStringInfo(join_request);
	pq_sendstring(join_request, request->nodegroup_name);
	pq_sendint(join_request, request->nodegroup_id, 4);
	pq_sendstring(join_request, request->joining_node_name);
	pq_sendint(join_request, request->joining_node_id, 4);
	pq_sendint(join_request, request->joining_node_state, 4);
	pq_sendstring(join_request, request->joining_node_if_name);
	pq_sendint(join_request, request->joining_node_if_id, 4);
	pq_sendstring(join_request, request->joining_node_if_dsn);
	pq_sendstring(join_request, request->join_target_node_name);
	pq_sendint(join_request, request->join_target_node_id, 4);
}

void
msg_deserialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request)
{
	request->nodegroup_name = pq_getmsgstring(join_request);
	request->nodegroup_id = pq_getmsgint(join_request, 4);
	request->joining_node_name = pq_getmsgstring(join_request);
	request->joining_node_id = pq_getmsgint(join_request, 4);
	request->joining_node_state = pq_getmsgint(join_request, 4);
	request->joining_node_if_name = pq_getmsgstring(join_request);
	request->joining_node_if_id = pq_getmsgint(join_request, 4);
	request->joining_node_if_dsn = pq_getmsgstring(join_request);
	request->join_target_node_name = pq_getmsgstring(join_request);
	request->join_target_node_id = pq_getmsgint(join_request, 4);
}
