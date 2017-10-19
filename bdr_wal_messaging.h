#ifndef BDR_WAL_MESSAGING_H
#define BDR_WAL_MESSAGING_H

extern void bdr_write_replay_progress_update(void);
extern void bdr_maybe_write_replay_progress_update(void);
extern void bdr_wal_messaging_register(void);

#endif
