#ifndef BDR_COMMANDS_TRIGGER_H
#define BDR_COMMANDS_TRIGGER_H

#include_next "commands/trigger.h"

/*
 * Pg commit a2e35b53 changed CreateTrigger's return type
 * from Oid to ObjectAddress. We can provide a fixup for this.
 *
 * Rely on gcc's statement expressions for this; see https://gcc.gnu.org/onlinedocs/gcc/Statement-Exprs.html,
 * so we don't need to use a different func name in BDR96.
 */

#define CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal) \
({ \
	ObjectAddress ret; \
	ret.classId = TriggerRelationId; \
	ret.objectSubId = 0; \
	ret.objectId = CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, isInternal); \
	ret; \
})

#endif
