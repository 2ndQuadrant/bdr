#ifndef BDR_UTILS_GUC_H
#define BDR_UTILS_GUC_H

#include_next "utils/guc.h"

#define GetConfigOptionByName(name, varname, missing_ok) \
(\
	AssertMacro(!missing_ok), \
	GetConfigOptionByName(name, varname) \
)

#endif
