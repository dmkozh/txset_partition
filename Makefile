CFLAGS = -O2 -g
CXXFLAGS = -O2 -g -std=c++17

txset_partition: cbitset.o txset_partition.o
	$(CXX) -o $@ $^

txset_partition.o: txset_partition.cpp BitSet.h cbitset.h cbitset_portability.h Makefile
	$(CXX) $(CXXFLAGS) -o $@ -c $<

cbitset.o: cbitset.c cbitset.h cbitset_portability.h Makefile
	$(CC) $(CFLAGS) -o $@ -c $<

clean:
	rm -f txset_partition txset_partition.o cbitset.o

