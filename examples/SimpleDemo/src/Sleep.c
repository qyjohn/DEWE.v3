#include <time.h>
#include <stdio.h>

int main()
{
        /* Intializes random number generator */
        time_t t;
        srand((unsigned) time(&t));

        /* Generate a random number between 0 and 100 */
        int time = rand() % 10;
        sleep(time);

	return 0;
}
