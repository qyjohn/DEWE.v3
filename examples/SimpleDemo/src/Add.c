#include <time.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	FILE *in1 = fopen(argv[1], "r");
	FILE *in2 = fopen(argv[2], "r");
	FILE *out = fopen(argv[3], "w");

	int a = 0;
	fscanf (in1, "%d", &a);
	fclose(in1);
        int b = 0;
        fscanf (in2, "%d", &b);
	fclose(in2);

	int c = a + b;
	fprintf(out, "%d", c);
	fclose(out);

	return 0;
}
