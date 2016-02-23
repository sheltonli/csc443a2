#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

int ram_size;
int block_size;
int num_runs;


typedef struct record {
  int uid1;
  int uid2;
} Record;

void phase1 (FILE* fp, int num_records);
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

  ram_size = atoi(argv[2]);
  block_size = atoi(argv[3]);
  num_runs = atoi(argv[4]);
  int num_records = ram_size/block_size*(block_size/sizeof(Record));

  phase1(fp, num_records);

  //pmms(fp);
  fclose(fp);

  return 0;
}


//pmms(FILE* fp)


void phase1 (FILE* fp, int num_records){
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    int num_chunks = fsize / num_runs;
    int size_of_chunk = fsize / num_chunks;

    int mem_used = 0;

    while (mem_used < ram_size){
        Record * buffer = (Record *) calloc (size_of_chunk, sizeof(Record));
        qsort (buffer, num_records, sizeof(Record), compare);

        mem_used += size_of_chunk;
        if (mem_used > ram_size){
            exit(EXIT_FAILURE);
        }

        free(buffer);
    }
    // Record * buffer = (Record *) calloc (num_records, sizeof(Record));
    // int result = fread (buffer, sizeof(Record), num_records, fp);
    // qsort (buffer, num_records, sizeof(Record), compare);
    // while(buffer!=NULL){
    //     printf(" %d\n", buffer->uid2);
    //     buffer ++;
    // }

    //write(1, buffer, num_records);
}