#ifndef BDR_WORKER_H
#define BDR_WORKER_H

#define BDR_VERSION_STR_SIZE 64

extern bool bdr_required_this_conn;
extern int peer_bdr_version_num;
extern char peer_bdr_version_str[BDR_VERSION_STR_SIZE];

#define PEER_HAS_BDR() (peer_bdr_version_num != -1)

extern void bdr_ensure_active_db(void);

#endif
