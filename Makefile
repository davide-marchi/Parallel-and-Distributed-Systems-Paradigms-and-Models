##############################################################################
#  Makefile
#  - builds all *.cpp listed in SRCS
#  - puts objects and dependency files in  	build/
#  - puts final executables in          	bin/
##############################################################################

# ------------------------------------------------------------------ compiler
CXX      ?= g++
MPICXX   ?= mpicxx                # only used for MPI targets

# ------------------------------------------------------------------ flags
CXXFLAGS += -std=c++20 -Wall
OPTFLAGS += -O3 -ffast-math
DBGFLAGS += -O0 -g -DDEBUG

ifeq ($(MODE),debug)
  CXXFLAGS += $(DBGFLAGS)
else
  CXXFLAGS += $(OPTFLAGS)
endif

INCLUDES += -I./fastflow          # FastFlow headers
LIBS     := -pthread
MPILIBS  := -lmpi

# ------------------------------------------------------------------ sources
SRCS := sequential.cpp \
        openmp.cpp     \
        fastflow.cpp   \
        io_comparison.cpp \
        sequential_onlymmap.cpp \
        sequential_nommap.cpp \
        # mpi_omp.cpp    \
        # mpi_ff.cpp

BINS := $(SRCS:.cpp=)

# ------------------------------------------------------------------ directories for artifacts
BUILD := build
BIN   := bin
$(shell mkdir -p $(BUILD) $(BIN))

CPP_O = $(BUILD)/$*.o
CPP_D = $(BUILD)/$*.d

# ------------------------------------------------------------------ generic pattern rules
$(BUILD)/%.o : %.cpp
	$(CXX) $(INCLUDES) $(CXXFLAGS) -MMD -MP -c $< -o $@

# Default link rule (sequential / OpenMP / FastFlow)
$(BIN)/%: $(BUILD)/%.o
	$(CXX) $^ -o $@ $(LIBS)

# ------------------------------------------------------------------ special rules

# openmp: CPP & link flags
$(BIN)/openmp: CXXFLAGS += -fopenmp
$(BIN)/openmp: LIBS     += -fopenmp

fastflow: INCLUDES += -I./fastflow

# mpi_omp: CXXFLAGS += -fopenmp
# mpi_omp: LIBS     += -fopenmp
# $(BIN)/mpi_omp: $(BUILD)/mpi_omp.o
#	$(MPICXX) $^ -o $@ $(MPILIBS) $(LIBS)
#
# mpi_ff: INCLUDES += -I./fastflow
# $(BIN)/mpi_ff: $(BUILD)/mpi_ff.o
#	$(MPICXX) $^ -o $@ $(MPILIBS) $(LIBS)

# ------------------------------------------------------------------ convenience targets
.PHONY: all clean distclean
all: $(addprefix $(BIN)/,$(BINS))

clean:
	$(RM) -r $(BUILD) *.dSYM

distclean: clean
	$(RM) -r $(BIN)

# ------------------------------------------------------------------ dependencies
-include $(SRCS:%.cpp=$(BUILD)/%.d)
