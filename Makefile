# Makefile for compiling process.cpp using C++17

# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -std=c++17 -Wall -Wextra -O2

# Target executable
TARGET = process

# Source file
SRC = process.cpp

# Default rule to build the target
all: $(TARGET)

# Rule to build the target executable
$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(SRC)

# Clean rule to remove the generated files
clean:
	rm -f $(TARGET)

# Phony targets
.PHONY: all clean
