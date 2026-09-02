#import "activity_bridge_darwin.h"

#import <CoreGraphics/CoreGraphics.h>
#import <Foundation/Foundation.h>
#import <math.h>

lms_activity_result lms_activity_read(void) {
    @autoreleasepool {
        double idle_seconds = CGEventSourceSecondsSinceLastEventType(
            kCGEventSourceStateCombinedSessionState,
            kCGAnyInputEventType
        );
        int32_t input_available = isfinite(idle_seconds) && idle_seconds >= 0.0;

        int32_t thermal_available = 1;
        int32_t thermal_unsafe = 0;
        switch ([NSProcessInfo processInfo].thermalState) {
            case NSProcessInfoThermalStateNominal:
            case NSProcessInfoThermalStateFair:
                break;
            case NSProcessInfoThermalStateSerious:
            case NSProcessInfoThermalStateCritical:
                thermal_unsafe = 1;
                break;
            default:
                thermal_available = 0;
                break;
        }

        return (lms_activity_result) {
            .idle_seconds = idle_seconds,
            .input_available = input_available,
            .thermal_available = thermal_available,
            .thermal_unsafe = thermal_unsafe,
        };
    }
}
