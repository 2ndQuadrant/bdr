#ifndef BDR_MSGFORMATS_H
#define BDR_MSGFORMATS_H

#define MAX_DIGITS_INT32 11
#define MAX_DIGITS_INT64 21

/*
 * A request to join a peer node.
 *
 * In some contexts not all fields are set; see
 * comments on usages.
 */
typedef struct BdrMsgJoinRequest
{
	const char *nodegroup_name;
	Oid			nodegroup_id;
	const char *joining_node_name;
	Oid			joining_node_id;
	int			joining_node_state;
	const char *joining_node_if_name;
	Oid			joining_node_if_id;
	const char *joining_node_if_dsn;
	const char *join_target_node_name;
	Oid			join_target_node_id;
} BdrMsgJoinRequest;

extern void msg_serialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request);
extern void msg_deserialize_join_request(StringInfo join_request,
	BdrMsgJoinRequest *request);
extern void msg_stringify_join_request(StringInfo out,
	BdrMsgJoinRequest *request);

extern void wrapInStringInfo(StringInfo si, char *data, Size length);

#endif
