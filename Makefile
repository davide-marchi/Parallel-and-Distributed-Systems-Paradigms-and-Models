.DEFAULT_GOAL := all

##############################################################################
#  Makefile
#  - builds all *.cpp listed in SRCS
#  - puts objects and dependency files in   build/
#  - puts final executables in              bin/
##############################################################################

# ------------------------------------------------------------------ compilers
CXX    ?= g++
MPICXX ?= mpicxx

# ------------------------------------------------------------------ flags (idiomatic split)
CPPFLAGS +=                          # preprocessor flags (e.g., -I, -D)
CXXFLAGS += -std=c++20 -Wall         # compile flags
OPTFLAGS += -O3 -ffast-math
DBGFLAGS += -O0 -g -DDEBUG
LDFLAGS  +=                          # linker flags
LDLIBS   += -pthread                 # libraries

# CPU tuning (default native; override with: make MARCH=x86-64-v3)
# MARCH ?= native
# OPTFLAGS += -march=$(MARCH)

# Mode toggle
ifeq ($(MODE),debug)
  CXXFLAGS += $(DBGFLAGS)
else
  CXXFLAGS += $(OPTFLAGS)
endif

# OpenMP switch (compile+link) for specific targets
OMPOPTS := -fopenmp

# FastFlow include path (apply only where needed)
FASTFLOW_CPPFLAGS := -I./fastflow

# ------------------------------------------------------------------ sources / bins
SRCS := openmp_seq_mmap.cpp \
        fastflow_seq_mmap.cpp \
        sequential_seq_mmap.cpp \
        mpi_omp_seq_mmap.cpp
#       mpi_ff.cpp

BINS := openmp_seq_mmap fastflow_seq_mmap sequential_seq_mmap mpi_omp_seq_mmap
#      mpi_ff

# ------------------------------------------------------------------ directories for artifacts
BUILD := build
BIN   := bin

# Keep object files; don't delete them as "intermediate"
OBJS := $(SRCS:%.cpp=$(BUILD)/%.o)
.SECONDARY: $(OBJS)

# ensure dirs exist (order-only prerequisites)
$(BUILD) $(BIN):
	mkdir -p $@

# ------------------------------------------------------------------ generic compile/link rules
$(BUILD)/%.o : %.cpp | $(BUILD)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -MMD -MP -c $< -o $@

$(BIN)/%: $(BUILD)/%.o | $(BIN)
	$(CXX) $(LDFLAGS) $^ -o $@ $(LDLIBS)

# ------------------------------------------------------------------ target-specific tweaks

# FastFlow TU needs only the FastFlow include at compile-time
$(BUILD)/fastflow_seq_mmap.o: CPPFLAGS += $(FASTFLOW_CPPFLAGS)
# Future FastFlow+MPI target
# $(BUILD)/mpi_ff.o:            CPPFLAGS += $(FASTFLOW_CPPFLAGS)

# OpenMP (sequential/OpenMP binary)
$(BUILD)/openmp_seq_mmap.o: CXXFLAGS += $(OMPOPTS)
$(BIN)/openmp_seq_mmap:     LDLIBS   += $(OMPOPTS)

# MPI+OpenMP: compile the TU with MPICXX (so <mpi.h> is found) and link with MPICXX
$(BUILD)/mpi_omp_seq_mmap.o: mpi_omp_seq_mmap.cpp | $(BUILD)
	$(MPICXX) $(CPPFLAGS) $(CXXFLAGS) $(OMPOPTS) -MMD -MP -c $< -o $@

$(BIN)/mpi_omp_seq_mmap: $(BUILD)/mpi_omp_seq_mmap.o | $(BIN)
	$(MPICXX) $(LDFLAGS) $^ -o $@ $(LDLIBS) $(OMPOPTS)

# Future: MPI+FastFlow binary
# $(BUILD)/mpi_ff.o: mpi_ff.cpp | $(BUILD)
# 	$(MPICXX) $(CPPFLAGS) $(FASTFLOW_CPPFLAGS) $(CXXFLAGS) $(OMPOPTS) -MMD -MP -c $< -o $@
# $(BIN)/mpi_ff: $(BUILD)/mpi_ff.o | $(BIN)
# 	$(MPICXX) $(LDFLAGS) $^ -o $@ $(LDLIBS) $(OMPOPTS)

# ------------------------------------------------------------------ convenience targets
.PHONY: all clean distclean
all: $(addprefix $(BIN)/,$(BINS))

clean:
	$(RM) -r $(BUILD) *.dSYM

distclean: clean
	$(RM) -r $(BIN)

# ------------------------------------------------------------------ dependencies
-include $(SRCS:%.cpp=$(BUILD)/%.d)
