#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define MB 1024*1024
#define KB 1024

typedef struct record {
  int uid1;
  int uid2;
} Record;

typedef struct inputBuffer{
  FILE* fp;
  int bufId;
  long totalElements; //how many total elements in buffer - may be different from max capacity
  long currentPositionInFile; 
  long currentBufferPosition;
  Record* buffer;
  int done;
} Buffer;

typedef struct max_avg{
  int avg;
  int max;
} Max_average;


long fsize;
int block_size;
int mem_size;
int num_runs;
int file_blocks;
int memory_blocks;
int memory_records;
int chunk_blocks;
int records_in_block;
int last_chunk_blocks;
int last_chunk_records;
int records_per_chunk;
int file_records;
int chunk_records;


void read_into_buffer(FILE * fp, Record * buffer);
void phase1 (FILE* fp);
void print_buffer_content (Record * buffer);
void phase2 ();
void phase3 ();
Buffer* initBuffer(FILE*fp, int id, long total_elem);


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
    printf("Usage: <name of the input file>, <total mem in MB>, <block size in KB>, <number of runs>\n");
    return 1;
  }
 
  if(!(fp = fopen(argv[1], "rb"))){
    exit(EXIT_FAILURE);
  }

  fseek(fp, 0, SEEK_END);
  fsize = ftell(fp);
  fseek(fp, 0, SEEK_SET);

  mem_size = MB*atoi(argv[2]);
  block_size = KB*atoi(argv[3]);
  num_runs = atoi(argv[4]);


  /* All sizes are in blocks */
  file_records = fsize/sizeof(Record);
  file_blocks = ceil(fsize/(float)block_size);

  memory_blocks = mem_size/block_size;
  memory_records = mem_size/sizeof(Record);

  chunk_blocks = ceil(file_blocks/(float)num_runs);
  chunk_records = chunk_blocks*records_in_block;
  records_in_block = block_size/sizeof(Record);

  if (chunk_blocks > memory_blocks + 1){
    printf("Not sufficient number of runs (%d) for %d MB memory \n", num_runs, mem_size/MB);
    return 1;  
  }
  records_per_chunk = chunk_blocks*records_in_block;

  phase1(fp);
  printf("file size: %li\n", fsize);
  printf("file block size: %d\n", block_size);
  printf("file size in blocks: %d\n", file_blocks);
  phase2();
  phase3();
  fclose(fp);

  return 0;
}


//r edges.dat 209715200 8192 10
void phase1 (FILE* fp){
    /* Allocate buffer for reading the file */
    Record * buffer = (Record *) calloc (records_per_chunk, sizeof(Record));
    FILE * fp_sorted;
    int count = 1;
    char file_name[20];

    /* Read file in chunks to the buffer */
    int result = fread (buffer, sizeof(Record), records_per_chunk, fp);
    while(result == records_per_chunk){
      //printf("Records in chunk %d: %d\n", count, result);
      sprintf(file_name, "output%d.dat", count);
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

    int last_chunk_records = file_records - (count-1)*records_per_chunk; 
    //printf("Records in chunk %d: %d\n", count, last_chunk_records);
    if(last_chunk_records){
      sprintf(file_name, "output%d.dat", count);
      if (!(fp_sorted = fopen (file_name, "wb" ))){
        exit(EXIT_FAILURE);
      }
      Record * small_buffer = (Record *) realloc(buffer, last_chunk_records*sizeof(Record));
      /* Sort in main memory */
      qsort (small_buffer, last_chunk_records, sizeof(Record), compare);
      /* Write sorted buffer to a new file */
      fwrite(small_buffer, sizeof(Record), last_chunk_records, fp_sorted);
      fclose(fp_sorted);
      free(small_buffer);
    }
    
}


void phase2(){
  /* Number of Records that can fit onto out buffers given memeory limitation*/
  /*+1 was added after dividing*/
  int buffer_blocks = memory_blocks/(num_runs+1);
  printf("buffer_blocks: %d\n", buffer_blocks);
  printf("mem_size: %d\n", mem_size);
  printf("mem_blocks: %d\n", memory_blocks);
  int buffer_records = buffer_blocks*records_in_block;
  printf("records_in_block: %d\n", records_in_block);

  printf("Number of records in each buffer: %ld\n", buffer_records);
  char file_name[20];
  FILE* out_file;
  if (!(out_file = fopen ("out_file.dat", "wb" ))){
        exit(EXIT_FAILURE);
  }

  /* should output buffer go here?*/
  Buffer* buffer_list[num_runs];

  /* heap */
  Record heap[num_runs];
  int heap_size = 0;

  for(int i = 1; i <= num_runs; i++){
    sprintf(file_name, "output%d.dat", i);
    FILE*fp;
    if (!(fp = fopen (file_name, "rb" ))){
        exit(EXIT_FAILURE);
    }
    buffer_list[i-1] = initBuffer(fp, i, buffer_records);

    /* read buffer_records from each input file into respective buffer */
    /*THIS DOESN'T WORK. BUFFER HAS BAD VALUES*/
    fread (buffer_list[i-1]->buffer, sizeof(Record), buffer_records, buffer_list[i-1]->fp);
    
    /* Initialize heap */
    heap[i-1] = buffer_list[i-1]->buffer[buffer_list[i-1]->currentBufferPosition];
    heap_size ++;
  }

  int min = heap[0].uid2;
  int min_id;

  Record* out_buf = (Record*)calloc(buffer_records, sizeof(Record));
  int outputBufCurPos = 0;

  while(heap_size != 0){
    /*for each buffer in a buffer list, 
    find smallest value and add to output buffer */
    for(int j = 0; j < num_runs; j++){
      if (heap[j].uid2 <= min){
        min = heap[j].uid2;
        min_id = j;
      }
    }
    /* get currentBufferPosition of buffer that contains smallest element in heap*/
    /* write to output buffer the smallest record */
    out_buf[outputBufCurPos] = buffer_list[min_id]->buffer[buffer_list[min_id]->currentBufferPosition];
    
    /* increment buffer positions of both output buffer and buffer that had smallest element */
    outputBufCurPos ++;
    buffer_list[min_id]->currentBufferPosition++;

    /* check if buffer that contained min element is empty
      if so, read from file and replace element in heap with next item
      else just replace with next item in buffer 
      reset buffer position */
    if (buffer_list[min_id]->currentBufferPosition >= buffer_records){
      int result = fread (buffer_list[min_id]->buffer, sizeof(Record), buffer_records, buffer_list[min_id]->fp);
      buffer_list[min_id]->currentBufferPosition = 0;
      if (result == 0){
        break;
      }
    }

    Record temp = buffer_list[min_id]->buffer[buffer_list[min_id]->currentBufferPosition];
    heap[min_id] = temp;

    /* reset min */
    min = heap[0].uid2;

    /* NEED SOME WAY OF HANDLING heap_size
    my idea was to subtract 1 from heap_size once it can't read anymore
    ie. getting a NULL record? not sure how to check null 
    since i keep elements of heap in place, might need to do a check in 
    min calculation that skips if the record is null*/

    /* check if output buffer is full, then do write 
      rest output buffer position */
    if (outputBufCurPos >= buffer_records){
      fwrite(out_buf, sizeof(Record), buffer_records, out_file);
      outputBufCurPos = 0;
    }
  }

  /* need to do write if there are still elements in out_buf 
    outputBufCurPos should be equal to # of records to be written*/
  if (outputBufCurPos > 0){
    fwrite(out_buf, sizeof(Record), outputBufCurPos, out_file);
  }
}

void phase3(){
  // FILE * fp;
  // if (!(fp = fopen ("out_file.dat", "rb" ))){
  //   exit(EXIT_FAILURE);
  // }
  // Record* buf = (Record*)calloc (file_records, sizeof(Record));
  // fread (buf, sizeof(Record), file_records, fp);

  // for (int i = 0; i < file_records; i++){
  //   printf("UID1: %d UID2: %d\n", buf[i].uid1, buf[i].uid2);
  // }

  FILE * fp_read;
  if (!(fp_read = fopen ("out_file.dat", "rb" ))){
    exit(EXIT_FAILURE);
  }

  Max_average ma;
  Record * buffer = (Record *) calloc (file_records, sizeof(Record));

  printf("File Records: %d\n", file_records);  

  int result = fread (buffer, sizeof(Record), file_records, fp_read);
  int current_conns = 0; 
  int max_conns = 0;
  int total_conns = 0;
  int current_id= -1;
  int last_id = -1;
  int total_ids = 0;

  for(int i = 0; i < file_records; i++){
    current_id = buffer[i].uid2;
    /* First loop */
    if (last_id == -1){
      last_id = current_id;
      total_ids ++;
    }

    if (current_id == last_id){
      current_conns ++;
    }
    else {
      total_ids ++;
      last_id = current_id;
      if (current_conns > max_conns){
        max_conns = current_conns;
      }
      current_conns = 1;
    }
    total_conns ++;
  }

  ma.avg = total_conns/total_ids;

  printf("%d\n", total_conns);
  printf("%d\n", total_ids);
  ma.max = max_conns;
  printf("Average number of connections: %d \t; Maximum number of connections: %d \n", ma.avg, ma.max);
  
}


Buffer* initBuffer(FILE*fp, int id, long total_elem){
  Buffer* b = (Buffer*)malloc(sizeof(Buffer));
  b->fp = fp;
  b->bufId = id;
  b->totalElements = total_elem;
  b->currentBufferPosition = 0;
  Record* buf = (Record*)calloc (total_elem, sizeof(Record));
  b->buffer = buf;
  b->done = 0;
  return b;
}


