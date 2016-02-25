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
int memory_size;

typedef struct record {
  int uid1;
  int uid2;
} Record;

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

  last_chunk = file_size % num_runs;
  records_in_last_chunk = last_chunk*block_size/sizeof(Record);


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
      //print_buffer_content (buffer);
      result = fread (buffer, sizeof(Record), records_per_chunk, fp);
    }
    // free(buffer);
    /* Read last chunk */
    if (last_chunk != 0){
      int buf_size = last_chunk*block_size*sizeof(Record);
      Record * small_buffer = (Record *) realloc(buffer, buf_size);
      /* Sort in main memory */
      qsort (small_buffer, records_in_last_chunk, sizeof(Record), compare);
      /* Write sorted buffer to a new file */
      fwrite(small_buffer, sizeof(Record), records_in_last_chunk, fp_sorted);
      free(small_buffer);
    } else {
      free(buffer);
    }
}


void phase2(FILE* fp){
  /* total input buffers = total number of blocks 
  tha can fit in memeory minus 1 block for output buffer */
  int total_input_buffers = num_runs-1;
  /* total number of blocks per buffer */
  int blocks_per_buffer = mem_size/num_runs;
  
  /* read one block from each chunk and put it into buffers*/
}


void print_buffer_content (Record * buffer){
  for (int i = 0; i < records_per_chunk; i++){
    printf("uid2: %d, ", (buffer + i)->uid2);
  }
}


