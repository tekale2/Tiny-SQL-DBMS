cc = g++ -std=c++11

all: TinySql
	rm *.o

TinySql: parser.o helper.o
	$(cc) -o TinySql parser.o helper.o main.cpp

parser.o: parser.h
	$(cc) -c parser.cpp

helper.o: helper.h
	$(cc) -c helper.cpp

clean:
	rm TinySql