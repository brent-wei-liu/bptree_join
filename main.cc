 
#include "Bruinbase.h"
#include "SqlEngine.h"

int main(int argc, char **argv)
{
    int argCount;
    char* debugArgs = "";
    for (argc--, argv++; argc > 0; argc -= argCount, argv += argCount) {
        argCount = 1;
        if (!strcmp(*argv, "-d")) {
            if (argc == 1)
            debugArgs = "+";    // turn on all debug flags
            else {
                debugArgs = *(argv + 1);
                argCount = 2;
            }
        }
    }
    DebugInit(debugArgs);           // initialize DEBUG messages
    // run the SQL engine taking user commands from standard input (console).
    SqlEngine::run(stdin);

    return 0;
}
