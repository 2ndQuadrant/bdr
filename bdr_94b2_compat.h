/* Backported to compile 0.9.1 with 9.4b2 */
typedef uint16 RepNodeId;
#define InvalidRepNodeId 0
#define DoNotReplicateRepNodeId USHRT_MAX

extern void GetReplicationInfoByIdentifierWrapper(RepNodeId riident, bool missing_ok, char **riname);

