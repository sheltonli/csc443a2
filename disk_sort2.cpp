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
void phase2 ();

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
  phase2();

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


void phase2(){
  /*  */
  int total_input_buffers = num_runs + 1;
  /* total number of blocks per buffer */
  int buffer_size = (mem_size/total_input_buffers) / block_size;
  int records_per_buffer = buffer_size / sizeof(Record);

  HeapRecord * heap = (HeapRecord *) calloc (num_runs, sizeof(HeapRecord));
  InputBuffer * inputBuffers = (InputBuffer *) calloc (num_runs, sizeof(InputBuffer));

  char file_name[20];
  for (int i = 1; i < num_runs + 1; i++){
    InputBuffer temp;
    sprintf(file_name, "output%d.dat", i);
    temp.init(records_per_buffer, file_name);
    inputBuffers[i] = temp;
  }

  // MergeManager manager;
  // manager.heap = heap;
  // manager.heapSize = 0;
  // manager.heapCapacity = num_runs;
  // FILE * inputFP;
  // manager.inputFP = inputFP;
  // FILE * outputFP;
  // if (!(outputFP = fopen ("final_sorted.txt", "wb" ))){
  //   exit(EXIT_FAILURE);
  // }
  // Record * outputBuffer = (Record *) calloc (buffer_size, sizeof(Record));
  // manager.outputBuffer = outputBuffer;
  // manager.currentPositionInOutputBuffer = 0;
  // manager.outputBufferCapacity = records_per_buffer;
  // manager.inputBuffers = inputBuffers;

  // for (int i = 1; i < num_runs + 1; i++){
  //   Record * temp;
  //   int result = insertIntoHeap(&manager, i, temp);
  //   printf("%d\n", result);
  // }


  // Record * result = (Record *) calloc (1, sizeof(Record));
  // inputBuffers[0].getNext(result);
  /* read one block from each chunk and put it into buffers*/
}

bool InputBuffer::init(int maxCapacity, const std::string inputFileName){
  const char * fname = inputFileName.c_str();
  if (!(file = fopen (fname, "wb" ))){
    return false;
  }

  buffer = (Record *) calloc (maxCapacity, sizeof(Record));
  int result = fread (buffer, sizeof(Record), maxCapacity, file);
  print_buffer_content(buffer);
  capacity = maxCapacity;
  currPositionInBuffer = 0;
  totalElementsInBuffer = maxCapacity;
  return true;
}

int InputBuffer::getNext(Record *result){
  result = &buffer[currPositionInBuffer];
  currPositionInBuffer ++;
  totalElementsInBuffer --;
  if (totalElementsInBuffer == 0){
    int result = fread(buffer, sizeof(Record), capacity, file);
    if (result == 0){
      return 2;
    }
  }
  return 1;
}


void print_buffer_content (Record * buffer){
  for (int i = 0; i < records_per_chunk; i++){
    printf("uid2: %d, ", (buffer + i)->uid2);
  }
}

// int getTopHeapElement (MergeManager *merger, HeapRecord *result) {
//   Record item;
//   int child, parent;

//   if(merger->heapSize == 0) {
//     printf( "UNEXPECTED ERROR: popping top element from an empty heap\n");
//     return 1;
//   }

//   *result=merger->heap[0];  //to be returned

//   //now we need to reorganize heap - keep the smallest on top
//   item = merger->heap [-- merger->heapSize]; // to be reinserted 

//   parent =0;

//   while ((child = (2 * parent) + 1) < merger->currentHeapSize) {
//     // if there are two children, compare them 
//     if (child + 1 < merger->heapSize && (compare((void *)&(merger->heap[child]),(void *)&(merger->heap[child + 1]))>0)) {
//       ++child;
//     }
//     // compare item with the larger 
//     if (compare((void *)&item, (void *)&(merger->heap[child]))>0) {
//       merger->heap[parent] = merger->heap[child];
//       parent = child;
//     } else {
//       break;
//     }
//   }
//   merger->heap[parent] = item;
  
//   return 0;
// }

int insertIntoHeap (MergeManager *merger, int run_id, Record *record) {
  HeapRecord hrecord;
  hrecord.uid1 = record ->uid1;
  hrecord.uid2 = record ->uid2;
  hrecord.run_id = run_id;
  
  int child, parent;
  if (merger->heapSize == merger->heapCapacity) {
    printf( "Unexpected ERROR: heap is full\n");
    return 1;
  } 
    
  child = merger->heapSize++; /* the next available slot in the heap */
  
  while (child > 0) {
    parent = (child - 1) / 2;
    if (compare((void *)&(merger->heap[parent]),(void *)&hrecord)>0) {
      merger->heap[child] = merger->heap[parent];
      child = parent;
    } else  {
      break;
    }
  }
  merger->heap[child]= hrecord;  
  return 0;
}

