CC      := clang
CFLAGS  := -Wall -g -pg -m64 -I./src -gdwarf-4 -mavx -O3
LDFLAGS := -debug -pthread -luuid -lzstd -luring -lm

OBJDIR := build
TARGET := $(OBJDIR)/test


ALL_SRCS := $(wildcard src/**/*.c)
SRCS := $(filter-out src/tests/% src/util/%, $(ALL_SRCS))
SRCS += $(wildcard src/db/backend/*.c)
SRCS += $(wildcard src/db/backend/dal/*.c)
SRCS += src/tests/testrunner.c
SRCS += src/tests/unity/src/unity.c
SRCS += src/util/io.c
SRCS += src/util/aco.c
SRCS += src/util/multitask.c 
SRCS += src/util/multitask_primitives.c
SRCS += src/util/maths.c 

# Add assembly file
ASM_SRCS := src/util/acosw.S
ASM_OBJS := $(notdir $(ASM_SRCS))
ASM_OBJS := $(ASM_OBJS:.S=.o)
ASM_OBJS := $(addprefix $(OBJDIR)/,$(ASM_OBJS))

OBJS   := $(notdir $(SRCS))
OBJS   := $(OBJS:.c=.o)
OBJS   := $(addprefix $(OBJDIR)/,$(OBJS))
OBJS   += $(ASM_OBJS)

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LDFLAGS)

define FIND_SRC
$(shell echo $(SRCS) | tr ' ' '\n' | grep '/$*.c$$')
endef

$(OBJDIR)/%.o:
	@mkdir -p $(OBJDIR)
	@echo "Compiling $* -> $@"
	$(CC) $(CFLAGS) -c $(call FIND_SRC,$*) -o $@

# Rule for compiling assembly files
$(OBJDIR)/acosw.o: src/util/acosw.S
	@mkdir -p $(OBJDIR)
	@echo "Assembling acosw.S -> $@"
	$(CC) -c $< -o $@

clean:
	rm -rf $(OBJDIR)

test: $(TARGET)
	@echo "Running tests..."
	@./$(TARGET)

.PHONY: all clean test
