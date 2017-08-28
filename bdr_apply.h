#ifndef BDR_APPLY_H
#define BDR_APPLY_H

#include "lib/stringinfo.h"

extern void bdr_receiver_writer_start(void);

extern void bdr_start_replication_params(StringInfo s);

extern bool bdr_handle_startup_param(const char *key, const char *value);

#endif
