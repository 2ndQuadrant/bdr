/*
 * xlog_internal.h
 *
 * PostgreSQL transaction log internal declarations
 *
 * NOTE: this file is intended to contain declarations useful for
 * manipulating the XLOG files directly, but it is not supposed to be
 * needed by rmgr routines (redo support for individual record types).
 * So the XLogRecord typedef and associated stuff appear in xlogrecord.h.
 *
 * Note: This file must be includable in both frontend and backend contexts,
 * to allow stand-alone tools like pg_receivexlog to deal with WAL files.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlog_internal.h
 */
#ifndef XLOG_INTERNAL_H
#define XLOG_INTERNAL_H

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "pgtime.h"
#include "storage/block.h"
#include "storage/relfilenode.h"


/*
 * Each page of XLOG file has a header like this:
 */
#define XLOG_PAGE_MAGIC 0xD081	/* can be used as WAL version indicator */

typedef struct XLogPageHeaderData
{
	uint16		xlp_magic;		/* magic value for correctness checks */
	uint16		xlp_info;		/* flag bits, see below */
	TimeLineID	xlp_tli;		/* TimeLineID of first record on page */
	XLogRecPtr	xlp_pageaddr;	/* XLOG address of this page */

	/*
	 * When there is not enough space on current page for whole record, we
	 * continue on the next page.  xlp_rem_len is the number of bytes
	 * remaining from a previous page.
	 *
	 * Note that xl_rem_len includes backup-block data; that is, it tracks
	 * xl_tot_len not xl_len in the initial header.  Also note that the
	 * continuation data isn't necessarily aligned.
	 */
	uint32		xlp_rem_len;	/* total len of remaining data for record */
} XLogPageHeaderData;

#define SizeOfXLogShortPHD	MAXALIGN(sizeof(XLogPageHeaderData))

typedef XLogPageHeaderData *XLogPageHeader;

/*
 * When the XLP_LONG_HEADER flag is set, we store additional fields in the
 * page header.  (This is ordinarily done just in the first page of an
 * XLOG file.)	The additional fields serve to identify the file accurately.
 */
typedef struct XLogLongPageHeaderData
{
	XLogPageHeaderData std;		/* standard header fields */
	uint64		xlp_sysid;		/* system identifier from pg_control */
	uint32		xlp_seg_size;	/* just as a cross-check */
	uint32		xlp_xlog_blcksz;	/* just as a cross-check */
} XLogLongPageHeaderData;

#define SizeOfXLogLongPHD	MAXALIGN(sizeof(XLogLongPageHeaderData))

typedef XLogLongPageHeaderData *XLogLongPageHeader;

/* When record crosses page boundary, set this flag in new page's header */
#define XLP_FIRST_IS_CONTRECORD		0x0001
/* This flag indicates a "long" page header */
#define XLP_LONG_HEADER				0x0002
/* This flag indicates backup blocks starting in this page are optional */
#define XLP_BKP_REMOVABLE			0x0004
/* All defined flag bits in xlp_info (used for validity checking of header) */
#define XLP_ALL_FLAGS				0x0007

#define XLogPageHeaderSize(hdr)		\
	(((hdr)->xlp_info & XLP_LONG_HEADER) ? SizeOfXLogLongPHD : SizeOfXLogShortPHD)

/*
 * The XLOG is split into WAL segments (physical files) of the size indicated
 * by XLOG_SEG_SIZE.
 */
#define XLogSegSize		((uint32) XLOG_SEG_SIZE)
#define XLogSegmentsPerXLogId	(UINT64CONST(0x100000000) / XLOG_SEG_SIZE)

#define XLogSegNoOffsetToRecPtr(segno, offset, dest) \
		(dest) = (segno) * XLOG_SEG_SIZE + (offset)

/*
 * Compute ID and segment from an XLogRecPtr.
 *
 * For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
 * a boundary byte is taken to be in the previous segment.  This is suitable
 * for deciding which segment to write given a pointer to a record end,
 * for example.
 */
#define XLByteToSeg(xlrp, logSegNo) \
	logSegNo = (xlrp) / XLogSegSize

#define XLByteToPrevSeg(xlrp, logSegNo) \
	logSegNo = ((xlrp) - 1) / XLogSegSize

/*
 * Is an XLogRecPtr within a particular XLOG segment?
 *
 * For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
 * a boundary byte is taken to be in the previous segment.
 */
#define XLByteInSeg(xlrp, logSegNo) \
	(((xlrp) / XLogSegSize) == (logSegNo))

#define XLByteInPrevSeg(xlrp, logSegNo) \
	((((xlrp) - 1) / XLogSegSize) == (logSegNo))

/* Check if an XLogRecPtr value is in a plausible range */
#define XRecOffIsValid(xlrp) \
		((xlrp) % XLOG_BLCKSZ >= SizeOfXLogShortPHD)

/*
 * The XLog directory and control file (relative to $PGDATA)
 */
#define XLOGDIR				"pg_xlog"
#define XLOG_CONTROL_FILE	"global/pg_control"

/*
 * These macros encapsulate knowledge about the exact layout of XLog file
 * names, timeline history file names, and archive-status file names.
 */
#define MAXFNAMELEN		64

#define XLogFileName(fname, tli, logSegNo)	\
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X", tli,		\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

#define XLogFromFileName(fname, tli, logSegNo)	\
	do {												\
		uint32 log;										\
		uint32 seg;										\
		sscanf(fname, "%08X%08X%08X", tli, &log, &seg); \
		*logSegNo = (uint64) log * XLogSegmentsPerXLogId + seg; \
	} while (0)

#define XLogFilePath(path, tli, logSegNo)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X", tli,				\
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId),				\
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId))

#define TLHistoryFileName(fname, tli)	\
	snprintf(fname, MAXFNAMELEN, "%08X.history", tli)

#define TLHistoryFilePath(path, tli)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X.history", tli)

#define StatusFilePath(path, xlog, suffix)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/archive_status/%s%s", xlog, suffix)

#define BackupHistoryFileName(fname, tli, logSegNo, offset) \
	snprintf(fname, MAXFNAMELEN, "%08X%08X%08X.%08X.backup", tli, \
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId),		  \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId), offset)

#define BackupHistoryFilePath(path, tli, logSegNo, offset)	\
	snprintf(path, MAXPGPATH, XLOGDIR "/%08X%08X%08X.%08X.backup", tli, \
			 (uint32) ((logSegNo) / XLogSegmentsPerXLogId), \
			 (uint32) ((logSegNo) % XLogSegmentsPerXLogId), offset)

/*
 * Information logged when we detect a change in one of the parameters
 * important for Hot Standby.
 */
typedef struct xl_parameter_change
{
	int			MaxConnections;
	int			max_worker_processes;
	int			max_prepared_xacts;
	int			max_locks_per_xact;
	int			wal_level;
	bool		wal_log_hints;
	bool		track_commit_timestamp;
} xl_parameter_change;

/* logs restore point */
typedef struct xl_restore_point
{
	TimestampTz rp_time;
	char		rp_name[MAXFNAMELEN];
} xl_restore_point;

/* End of recovery mark, when we don't do an END_OF_RECOVERY checkpoint */
typedef struct xl_end_of_recovery
{
	TimestampTz end_time;
	TimeLineID	ThisTimeLineID; /* new TLI */
	TimeLineID	PrevTimeLineID; /* previous TLI we forked off from */
} xl_end_of_recovery;

/*
 * The functions in xloginsert.c construct a chain of XLogRecData structs
 * to represent the final WAL record.
 */
typedef struct XLogRecData
{
	struct XLogRecData *next;	/* next struct in chain, or NULL */
	char	   *data;			/* start of rmgr data to include */
	uint32		len;			/* length of rmgr data to include */
} XLogRecData;

/*
 * Recovery target action.
 */
typedef enum
{
	RECOVERY_TARGET_ACTION_PAUSE,
	RECOVERY_TARGET_ACTION_PROMOTE,
	RECOVERY_TARGET_ACTION_SHUTDOWN,
} RecoveryTargetAction;

/*
 * Method table for resource managers.
 *
 * This struct must be kept in sync with the PG_RMGR definition in
 * rmgr.c.
 *
 * rm_identify must return a name for the record based on xl_info (without
 * reference to the rmid). For example, XLOG_BTREE_VACUUM would be named
 * "VACUUM". rm_desc can then be called to obtain additional detail for the
 * record, if available (e.g. the last block).
 *
 * RmgrTable[] is indexed by RmgrId values (see rmgrlist.h).
 */
typedef struct RmgrData
{
	const char *rm_name;
	void		(*rm_redo) (XLogReaderState *record);
	void		(*rm_desc) (StringInfo buf, XLogReaderState *record);
	const char *(*rm_identify) (uint8 info);
	void		(*rm_startup) (void);
	void		(*rm_cleanup) (void);
} RmgrData;

extern const RmgrData RmgrTable[];

/*
 * Exported to support xlog switching from checkpointer
 */
extern pg_time_t GetLastSegSwitchTime(void);
extern XLogRecPtr RequestXLogSwitch(void);

extern void GetOldestRestartPoint(XLogRecPtr *oldrecptr, TimeLineID *oldtli);

/*
 * Exported for the functions in timeline.c and xlogarchive.c.  Only valid
 * in the startup process.
 */
extern bool ArchiveRecoveryRequested;
extern bool InArchiveRecovery;
extern bool StandbyMode;
extern char *recoveryRestoreCommand;

/*
 * Prototypes for functions in xlogarchive.c
 */
extern bool RestoreArchivedFile(char *path, const char *xlogfname,
					const char *recovername, off_t expectedSize,
					bool cleanupEnabled);
extern void ExecuteRecoveryCommand(char *command, char *commandName,
					   bool failOnerror);
extern void KeepFileRestoredFromArchive(char *path, char *xlogfname);
extern void XLogArchiveNotify(const char *xlog);
extern void XLogArchiveNotifySeg(XLogSegNo segno);
extern void XLogArchiveForceDone(const char *xlog);
extern bool XLogArchiveCheckDone(const char *xlog);
extern bool XLogArchiveIsBusy(const char *xlog);
extern void XLogArchiveCleanup(const char *xlog);

#endif   /* XLOG_INTERNAL_H */
