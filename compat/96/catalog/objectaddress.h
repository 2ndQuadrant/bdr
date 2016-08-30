#ifndef BDR_COMPAT_CATALOG_OBJECTADDRESS_H
#define BDR_COMPAT_CATALOG_OBJECTADDRESS_H

/*
 * 9.4bdr commit dab9ecbd (bdr-drops) added unstringify_objtype(...).
 *
 * This didn't go in with the DDL replicaiton hooks in 9.6.
 */

#include_next "catalog/objectaddress.h"

inline static int unstringify_objtype(const char *objtype)
{
	elog(FATAL, "DDL replication not supported in 9.6 yet");
}

#endif
