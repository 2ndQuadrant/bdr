#ifndef PGNAMESPACE_CATALOG_COMPAT_H
#define PGNAMESPACE_CATALOG_COMPAT_H

#include_next "catalog/namespace.h"

/* 73fe87503 renamed isTempOrToastNamespace to isTempOrTempToastNamespace */
#define isTempOrTempToastNamespace(x) isTempOrToastNamespace(x)

#endif
