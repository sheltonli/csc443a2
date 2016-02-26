#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Merge.h"

int mem_size;
int block_size;
int num_runs;
int file_size;
int chunk_size;
int records_per_chunk;
int excess_blocks;
int records_in_last_chunk;
int memory_size;
int last_chunk;

void read_into_buffer(FILE * fp, Record * buffer);
void phase1 (FILE* fp);
void print_buffer_content (Record * buffer);

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


  /* All sizes are in blocks */
  file_size = fsize/block_size;
  memory_size = mem_size/block_size;

  chunk_size = file_size/num_runs;
  records_per_chunk = chunk_size*block_size/sizeof(Record);

  /* So, instead of partitioning the file into k chunks and having
    one extra chunk, it probably makes sense to hae exactly k chunks (for part 2).

    My thoughts, might be a better way:
    1. Calculate the remainder (extra blocks).
    2. Add these extra blocks to chunk_size.
    3. Calculate the new records_per_chunk.
    4. Do calculations for exact size of the k-th chunk (this one won't hold the maximum).
    5. In phase 1, it will now make an output file for each chunk.
   */
  excess_blocks = file_size % num_runs;
  if (excess_blocks != 0){
    chunk_size = chunk_size + excess_blocks;
    records_per_chunk = chunk_size*block_size/sizeof(Record);

    last_chunk = file_size - (num_runs - 1) * chunk_size;
    records_in_last_chunk = last_chunk*block_size/sizeof(Record);
  }

  /* chunk size should be 1 block smaller than main memory size */
  if ((records_per_chunk > mem_size) || ((records_per_chunk == mem_size) && (records_in_last_chunk > mem_size % block_size))){
    printf("Not enough runs \n");
    return 1;
  }

  phase1(fp);

  //pmms(fp);
  fclose(fp);

  return 0;
}


//r edges.dat 209715200 8192 10
void phase1 (FILE* fp){
    /* Allocate buffer for reading the file */
    Record * buffer = (Record *) calloc (records_per_chunk, sizeof(Record));
    
    /* Read file in chunks to the buffer */
    int result = fread (buffer, sizeof(Record), records_per_chunk, fp);


    int count = 1;
    char file_name[20];
    while(result == records_per_chunk){
      sprintf(file_name, "output%d.dat", count);
      FILE * fp_sorted;
      if (!(fp_sorted = fopen (file_name, "wb" ))){
        exit(EXIT_FAILURE);
      }
        
      /* Sort in main memory */
      qsort (buffer, records_per_chunk, sizeof(Record), compare);
      /* Write sorted buffer to a new file */
      fwrite(buffer, sizeof(Record), records_per_chunk, fp_sorted);
      //print_buffer_content (buffer);
      result = fread (buffer, sizeof(Record), records_per_chunk, fp);
      count ++;
      fclose(fp_sorted);
    }

    /* Read last chunk */
    if (excess_blocks != 0){
      sprintf(file_name, "output%d.dat", count);
      FILE * fp_sorted;
      if (!(fp_sorted = fopen (file_name, "wb" ))){
        exit(EXIT_FAILURE);
      }
      int buf_size = last_chunk*block_size*sizeof(Record);
      Record * small_buffer = (Record *) realloc(buffer, buf_size);
      /* Sort in main memory */
      qsort (small_buffer, records_in_last_chunk, sizeof(Record), compare);
      /* Write sorted buffer to a new file */
      fwrite(small_buffer, sizeof(Record), records_in_last_chunk, fp_sorted);
      fclose(fp_sorted);
      free(small_buffer);
    } else {
      free(buffer);
    }
}


void phase2(FILE* fp){
  /*  */
  int total_input_buffers = num_runs + 1;
  /* total number of blocks per buffer */
  int buffer_size = (mem_size/total_input_buffers) / block_size;
  int records_per_buffer = buffer_size / sizeof(Record)

  // for (int i = 0; i < num_runs; i++){
    
  // }  
  /* read one block from each chunk and put it into buffers*/
}


void print_buffer_content (Record * buffer){
  for (int i = 0; i < records_per_chunk; i++){
    printf("uid2: %d, ", (buffer + i)->uid2);
  }
}


