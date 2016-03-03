CC = g++

all: disk_sort
 
disk_sort: disk_sort.cpp
	$(CC) -o $@ $<


clean: 
	rm disk_sort out_file.dat output*.dat
    
