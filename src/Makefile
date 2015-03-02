all: reference

# -Wall
reference: reference.cpp
	g++ -std=c++11 -O3 -g -march=corei7 -Wextra -o$@ reference.cpp

clean:
	rm reference