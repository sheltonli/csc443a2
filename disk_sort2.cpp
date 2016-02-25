#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int mem_size;
int block_size;
int num_runs;
int file_size;
int chunk_size;
int records_per_chunk;
int last_chunk;
int records_in_last_chunk;

typedef struct record {
  int uid1;
  int uid2;
} Record;

void read_into_buffer(FILE * fp, Record * buffer);
void phase1 (FILE* fp);


/**
* Compares two records a and b 
* with respect to the value of the integer field f.
* Returns an integer which indicates relative order: 
* positive: record a > record b
* negative: record a < record b
* zero: equal records
*/
int compare (const void *a, const void *b) {
    int a_uid2 = ((Record *)a)->uid2;
    int b_uid2 = ((Record *)b)->uid2;
    return (a_uid2 - b_uid2);
}

int main(int argc, char const *argv[])
{
  FILE* fp;

  if (argc != 5){
    printf("Usage: <name of the input file>, <total mem in bytes>, <block size>, <number of runs>\n");
    return 1;
  }
 
  if(!(fp = fopen(argv[1], "rb"))){
    exit(EXIT_FAILURE);
  }

  fseek(fp, 0, SEEK_END);
  long fsize = ftell(fp);
  fseek(fp, 0, SEEK_SET);

  mem_size = atoi(argv[2]);
  block_size = atoi(argv[3]);
  num_runs = atoi(argv[4]);


  /* All sizes in blocks */
	file_size = fsize/block_size;

  chunk_size = file_size/num_runs;
  records_per_chunk = chunk_size*block_size/sizeof(Record);

  last_chunk = file_size % num_runs;
  records_in_last_chunk = last_chunk*block_size/sizeof(Record);


  /* COMMENT FOR VALERY 
    Don't we want to check records_per_chunk against mem_size?
    We are going to allocate memory of size records_per_chunk.
    That's the true size of the chunk and not chunk_size?
    Same applies for the 2nd part of the OR where we need to check records_in_last_chunk.
  */
  if ((records_per_chunk > mem_size) || ((records_per_chunk == mem_size) && (records_in_last_chunk > mem_size % block_size))){
  	printf("Not enough runs \n");
  	return 1;
  }

  phase1(fp);

  //pmms(fp);
  fclose(fp);

  return 0;
}


//pmms(FILE* fp)


void phase1 (FILE* fp){
	  FILE * fp_sorted;
  	if (!(fp_sorted = fopen ("sorted.dat", "wb" )))
    	exit(EXIT_FAILURE);

 	  /* Allocate buffer for reading the file */
 		Record * buffer = (Record *) calloc (records_per_chunk, sizeof(Record));

    /* Read file in chunks to the buffer */
    int result = fread (buffer, sizeof(Record), records_per_chunk, fp);

		while(result == records_per_chunk){
			/* Sort in main memory */
			qsort (buffer, records_per_chunk, sizeof(Record), compare);
			/* Write sorted buffer to a new file */
			fwrite(buffer, sizeof(Record), records_per_chunk, fp_sorted);
			result = fread (buffer, sizeof(Record), records_per_chunk, fp);
		}
		/* Read last chunk */
		if (last_chunk != 0){
			
      /* NOTE FOR VALERY 
        Before it was doing another calculation of last_chunk*block_size*sizeof(Record).
        Not sure why it was multiplying by sizeof(Record).
        I'm assuming you wanted to do a calculation like the ones above (for records_per_chunk and records_in_last_chunk).
        If that's the case, then we can just use records_in_last_chunk?
      */

			Record * small_buffer = (Record *) realloc(buffer, records_in_last_chunk);

      /* NOTE FOR VALERY 
        Don't we want to sort the remaining records which is of size records_in_last_chunk?
      */
			qsort (small_buffer, records_in_last_chunk, sizeof(Record), compare);
			
      /* NOTE FOR VALERY 
        Aren't we writing what's in small_buffer?
      */
			fwrite(small_buffer, sizeof(Record), records_in_last_chunk, fp_sorted);

      /* NOTE FOR VALERY
        I think we should free the memory. This way is okay right?
      */
      free(small_buffer);
		}
    else {
      free(buffer);
    }
   
}





