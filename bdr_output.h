#ifndef BDR_OUTPUT_H
#define BDR_OUTPUT_H

struct LogicalDecodingContext;
struct OutputPluginOptions;

extern void bdr_output_start(struct LogicalDecodingContext * ctx, struct OutputPluginOptions *opt);

extern void bdr_output_shutdown(struct LogicalDecodingContext * ctx);

struct DefElem;

extern bool bdr_process_output_params(struct DefElem *elem);

extern List* bdr_prepare_startup_params(List *params);

#endif
