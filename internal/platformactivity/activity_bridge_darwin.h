#ifndef LMS_ACTIVITY_BRIDGE_DARWIN_H
#define LMS_ACTIVITY_BRIDGE_DARWIN_H

#include <stdint.h>

typedef struct {
    double idle_seconds;
    int32_t input_available;
    int32_t thermal_available;
    int32_t thermal_unsafe;
} lms_activity_result;

lms_activity_result lms_activity_read(void);

#endif
