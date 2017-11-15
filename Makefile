cc = g++ -std=c++11

all: TinySql
	rm *.o

TinySql: parser.o 
	$(cc) -o TinySql parser.o main.cpp

parser.o: parser.h
	$(cc) -c parser.cpp

clean:
	rm TinySql