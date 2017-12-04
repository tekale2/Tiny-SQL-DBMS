INTRO:-> Final Project for CSCE 608 Database Systems FALL 2017

Requirements:-> 
	The whole project is written in C++ with C++11 features. Tested on ubuntu 16.04 with gcc version 5.4.0. 

Changes to Library:->
The following changes were made to the provided StorageManager library:
C++11 requires constexpr and hence 3 lines Disk.h in StorageManger library have been changed to:
static constexpr double avg_seek_time=6.46;
static constexpr double avg_rotation_latency=4.17;
static constexpr double avg_transfer_time_per_block=0.20 * 320;

Changes to TestFile:->
1. The provided testfile TinySQL_linux.txt had Windows' line endings which caused errors in parsing. The line endings wre converted to linux line endings.

2."NOT", "/",[", "]" operators were not included in grammar. Hence test queries which involed the characters were changed to reflect equivalent logical query. i.e changing divison logic with equivalnet multiplication logic and changing to equivalent boolean expression.

Build:->
To build the project just run make command

Running the project:->
There are 2 ways of executing the program:
1> can run with input from file as:
	./TinySql TinySQL_linux.txt
 and everyting will be sent to stdout. Output to log file can be saved as follows:
	./TinySql TinySQL_linux.txt > log.txt
  will save the output to log file

2> Commands can be typed and those queries will be executed:
	./TinySql
 and everyting will be sent to stdout. Output to log file can be saved as follows:
	./TinySql > log.txt
  will save the output to log file
  Typing QUIT or q will exit from the program

