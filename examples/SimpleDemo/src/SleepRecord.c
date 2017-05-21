#include <time.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	/* Intializes random number generator */
	time_t t;
	srand((unsigned) time(&t));

	/* Generate a random number between 0 and 100 */
	int time = rand() % 10;
	sleep(time);

	FILE *file = fopen(argv[1], "w");
	fprintf(file, "%i", time);
	fclose(file);

	return 0;
}
