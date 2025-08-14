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
# NOTE: assumes you renamed the source files accordingly.
SRCS := omp_seq_mmap.cpp \
		omp_mmap.cpp \
        ff_seq_mmap.cpp \
		ff_mmap.cpp \
        sequential_seq_mmap.cpp \
        mpi_omp_seq_mmap.cpp \
        mpi_omp_mmap.cpp
#       mpi_ff.cpp

BINS := omp_seq_mmap omp_mmap ff_seq_mmap ff_mmap sequential_seq_mmap mpi_omp_seq_mmap mpi_omp_mmap
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

# Any ff_* TU gets FastFlow include at compile-time
$(BUILD)/ff_%.o: CPPFLAGS += $(FASTFLOW_CPPFLAGS)

# Any omp_* target gets OpenMP at compile and link
$(BUILD)/omp_%.o: CXXFLAGS += $(OMPOPTS)
$(BIN)/omp_%:     LDLIBS   += $(OMPOPTS)

# MPI+OpenMP (compile/link with MPICXX and -fopenmp)
$(BUILD)/mpi_omp_%.o: CXX := $(MPICXX)
$(BUILD)/mpi_omp_%.o: CXXFLAGS += $(OMPOPTS)

$(BIN)/mpi_omp_%: CXX := $(MPICXX)
$(BIN)/mpi_omp_%: LDLIBS += $(OMPOPTS)

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
