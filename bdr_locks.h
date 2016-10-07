/*
 * bdr_locks.h
 *
 * BiDirectionalReplication
 *
 * Copyright (c) 2014-2015, PostgreSQL Global Development Group
 *
 * bdr_locks.h
 */
#ifndef BDR_LOCKS_H
#define BDR_LOCKS_H

typedef enum BDRLockType
{
	BDR_LOCK_NOLOCK = 0,	/* no lock (not used) */
	BDR_LOCK_DDL = 1,		/* lock against DDL */
	BDR_LOCK_WRITE = 2		/* lock against any write */
} BDRLockType;

void bdr_locks_startup(void);
void bdr_locks_set_nnodes(Size nnodes);
void bdr_acquire_ddl_lock(BDRLockType lock_type);
void bdr_process_acquire_ddl_lock(uint64 sysid, TimeLineID tli, Oid datid,
								  BDRLockType lock_type);
void bdr_process_release_ddl_lock(uint64 sysid, TimeLineID tli, Oid datid,
								  uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid);
void bdr_process_confirm_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
								  uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid,
								  BDRLockType lock_type);
void bdr_process_decline_ddl_lock(uint64 origin_sysid, TimeLineID origin_tli, Oid origin_datid,
								  uint64 lock_sysid, TimeLineID lock_tli, Oid lock_datid,
								  BDRLockType lock_type);
void bdr_process_request_replay_confirm(uint64 sysid, TimeLineID tli, Oid datid, XLogRecPtr lsn);
void bdr_process_replay_confirm(uint64 sysid, TimeLineID tli, Oid datid, XLogRecPtr lsn);
void bdr_locks_process_remote_startup(uint64 sysid, TimeLineID tli, Oid datid);

extern bool bdr_locks_process_message(int msg_type, bool transactional,
									  XLogRecPtr lsn, uint64 origin_sysid,
									  TimeLineID origin_tlid, Oid origin_datid,
									  StringInfo message);

#endif
