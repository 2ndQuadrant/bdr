#ifndef PGVERSION_COMPAT_H
#define PGVERSION_COMPAT_H

#include_next "executor/executor.h"

#define ExecOpenIndices(resultRelInfo, speculative) \
( \
	AssertMacro(!speculative), \
	ExecOpenIndices(resultRelInfo) \
)

#define ExecInsertIndexTuples(slot, tupleid, estate, noDupErr, specConflict, arbiterIndexes) \
( \
	AssertMacro(!noDupErr), \
	AssertMacro(specConflict == NULL), \
	AssertMacro(arbiterIndexes == NIL), \
	ExecInsertIndexTuples(slot, tupleid, estate) \
)

#endif
