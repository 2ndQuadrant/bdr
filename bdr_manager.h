#ifndef BDR_MANAGER_H
#define BDR_MANAGER_H

extern int bdr_max_nodes;

extern void bdr_manager_worker_start(void);

struct WaitEvent;
extern void bdr_manager_wait_event(struct WaitEvent *events, int nevents,
								   long  *max_next_wait_ms);

extern void bdr_wait_event_set_recreated(struct WaitEventSet *new_set);

extern int bdr_get_wait_event_space_needed(void);

#endif
