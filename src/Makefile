all: reference

# -Wall
reference: reference.cpp
	g++ -std=c++11 -O3 -ffast-math -pg -march=native -mfpmath=sse -Wextra -o$@ reference.cpp

clean:
	rm reference