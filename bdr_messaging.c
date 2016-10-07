/* -------------------------------------------------------------------------
 *
 * bdr_messaging.c
 *		Replication!!!
 *
 * Replication???
 *
 * Copyright (C) 2012-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		bdr_messaging.c
 *
 * BDR needs to do cluster-wide operations with varying degrees of synchronous
 * behaviour in order to perform DDL, part/join nodes, etc. Operations may need
 * to communicate with a quorum of nodes or all known nodes. The logic to
 * handle WAL message sending/receiving and dispatch, quorum counting, etc is
 * centralized here.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bdr.h"
#include "bdr_locks.h"
#include "bdr_messaging.h"

#include "libpq/pqformat.h"

#include "replication/message.h"
#include "replication/origin.h"

#include "miscadmin.h"

/*
 * Receive and decode a logical WAL message
 */
void
bdr_process_remote_message(StringInfo s)
{
#if PG_VERSION_NUM/100 == 904
	int			chanlen;
	const char *chan;
#endif
	StringInfoData message;
	bool		transactional;
	int			msg_type;
	XLogRecPtr	lsn;
	uint64		origin_sysid;
	TimeLineID	origin_tlid;
	Oid			origin_datid;
	int			origin_namelen;

	transactional = pq_getmsgbyte(s);
	lsn = pq_getmsgint64(s);

	/*
	 * Logical WAL messages are (for some reason) encapsulated in their own
	 * header with its own length, even though the outer CopyData knows its
	 * length. Unwrap it.
	 */
	initStringInfo(&message);
	message.len = pq_getmsgint(s, 4);
	message.data = (char *) pq_getmsgbytes(s, message.len);

#if PG_VERSION_NUM/100 == 904
	/*
	 * 9.4 carried message multiplexing info in the payload, so we have to
	 * examine and discard it after making sure the message is for us.
	 *
	 * Even though there are no other channel users this must be retained
	 * for compatibility with older 9.4bdr nodes.
	 */

	chanlen = pq_getmsgint(&message, 4);
	chan = pq_getmsgbytes(&message, chanlen);

	/* Channel filtering is only needed in 9.4, in 9.6 it's done on the output plugin */
	if (strncmp(chan, "bdr", chanlen) != 0)
	{
		elog(LOG, "ignoring message in channel %s",
			 pnstrdup(chan, chanlen));
		return;
	}

	/*
	 * The message is for us. The un-consumed portion of the 'message'
	 * StringInfo is the same as the body of a 9.6 WAL message now.
	 */
#endif

	msg_type = pq_getmsgint(&message, 4);
	origin_sysid = pq_getmsgint64(&message);
	origin_tlid = pq_getmsgint(&message, 4);
	origin_datid = pq_getmsgint(&message, 4);
	origin_namelen = pq_getmsgint(&message, 4);

	elog(DEBUG1, "message type %s from "UINT64_FORMAT":%u database %u at %X/%X",
		 bdr_message_type_str(msg_type), origin_sysid, origin_tlid, origin_datid,
		 (uint32) (lsn >> 32), (uint32) lsn);

	if (origin_namelen != 0)
		elog(ERROR, "no names expected yet");

	if (bdr_locks_process_message(msg_type, transactional, lsn, origin_sysid, origin_tlid, origin_datid, &message))
		goto done;
	
	elog(WARNING, "unhandled BDR message of type %s", bdr_message_type_str(msg_type));

	resetStringInfo(&message);

done:
	if (!transactional)
		replorigin_session_advance(lsn, InvalidXLogRecPtr);
}

/*
 * Prepare a StringInfo with a BDR WAL-message header. The caller
 * should then append message-specific payload to the StringInfo
 * with the pq_send functions, then call bdr_send_message(...)
 * to dispatch it.
 *
 * The StringInfo must be initialized.
 */
void
bdr_prepare_message(StringInfo s, BdrMessageType message_type)
{
#if PG_VERSION_NUM/100 == 904
	/* channel. Only send on 9.4 since it's embedded in 9.6 messages */
	pq_sendint(s, strlen(BDR_LOGICAL_MSG_PREFIX), 4);
	pq_sendbytes(s, BDR_LOGICAL_MSG_PREFIX, strlen(BDR_LOGICAL_MSG_PREFIX));
#endif
	/* message type */
	pq_sendint(s, message_type, 4);
	/* node identifier */
	pq_sendint64(s, GetSystemIdentifier()); /* sysid */
	pq_sendint(s, ThisTimeLineID, 4); /* tli */
	pq_sendint(s, MyDatabaseId, 4); /* database */
	pq_sendint(s, 0, 4); /* name, always empty for now */

	/* caller's data will follow */
}

/*
 * Send a WAL message previously prepared with bdr_prepare_message,
 * after using pq_send functions to add message-specific payload.
 * 
 * The StringInfo is reset automatically and may be re-used
 * for another message.
 */
void
bdr_send_message(StringInfo s, bool transactional)
{
	XLogRecPtr lsn;

	lsn = LogLogicalMessage(BDR_LOGICAL_MSG_PREFIX, s->data, s->len, transactional);
	XLogFlush(lsn);
	resetStringInfo(s);
}

/*
 * Get the text name for a message type. The caller must
 * NOT free the result.
 */
char* bdr_message_type_str(BdrMessageType message_type)
{
	switch (message_type)
	{
		case BDR_MESSAGE_START:
			return "BDR_MESSAGE_START";
		case BDR_MESSAGE_ACQUIRE_LOCK:
			return "BDR_MESSAGE_ACQUIRE_LOCK";
		case BDR_MESSAGE_RELEASE_LOCK:
			return "BDR_MESSAGE_RELEASE_LOCK";
		case BDR_MESSAGE_CONFIRM_LOCK:
			return "BDR_MESSAGE_CONFIRM_LOCK";
		case BDR_MESSAGE_DECLINE_LOCK:
			return "BDR_MESSAGE_DECLINE_LOCK";
		case BDR_MESSAGE_REQUEST_REPLAY_CONFIRM:
			return "BDR_MESSAGE_REQUEST_REPLAY_CONFIRM";
		case BDR_MESSAGE_REPLAY_CONFIRM:
			return "BDR_MESSAGE_REPLAY_CONFIRM";
	}
	elog(ERROR, "unhandled BdrMessageType %d", message_type);
}
