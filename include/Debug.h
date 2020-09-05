/**
    @file debug helper functions.
 */
/*** Debug header. ***/

/** Version 1 + Functional Model Modification **/

/** Redundance check. **/
#ifndef DEBUG_HEADER
#define DEBUG_HEADER

/** Included files. **/
#include <stdarg.h>   /* Standard argument operations. E.g. va_list */
#include <stdio.h>    /* Standard I/O operations. E.g. vprintf() */
#include <sys/time.h> /* Time functions. E.g. gettimeofday() */

/** Defninitions. **/
namespace config
{
namespace debug
{
constexpr static int MAX_FORMAT_LEN = 255;
constexpr static bool TITLE = false;
constexpr static bool TIMER = false;
constexpr static bool CUR = false;
}  // namespace debug
}  // namespace config

/** Classes. **/

class Debug
{
private:
    static long startTime; /* Last start time in milliseconds. */

public:
    static void debugTitle(const char *str); /* Print debug title string. */
    static void debugItem(const char *format,
                          ...); /* Print debug item string. */
    static void debugCur(const char *format,
                         ...); /* Print debug item string. */
    static void notifyDebug(const char *format, ...);
    static void notifyInfo(const char *format,
                           ...); /* Print normal notification. */
    static void notifyError(const char *format,
                            ...); /* Print error information. */
    static void notifyPanic(const char *format, ...);
    static void startTimer(const char *); /* Start timer and display
                                             information. */
    static void endTimer(); /* End timer and display information. */

    static void check(bool cond,
                      const char *format,
                      ...); /* check that an assertion holds */
    static void dcheck(bool cond,
                       const char *format,
                       ...); /* check that an assertion holds on debug mode */
};

/** Redundance check. **/

#endif
