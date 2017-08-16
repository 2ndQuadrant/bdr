#ifndef BDR_MANAGER_H
#define BDR_MANAGER_H

extern int max_bdr_nodes;

extern void bdr_manager_worker_start(void);

struct WaitEvent;
extern void bdr_manager_wait_event(struct WaitEvent *events, int nevents);

#endif
