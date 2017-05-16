CC      := g++
CFLAGS  := -pthread -Wall -O -std=gnu++11 -g
INCLUDEFLAGS = -I./include
LDFLAGS := -lpcap -lboost_system -lssl 
TARGETS := pm_monitor pm_daemon 
CXXFLAGS := $(CFLAGS) $(INCLUDEFLAGS)

######################Setting Varialbes#######################################

BUILD_DIR := build

MONITOR_SRCS := $(shell find src/monitor/ -name "*.cc")
MONITOR_OBJS := $(sort $(addprefix $(BUILD_DIR)/, $(MONITOR_SRCS:.cc=.o)))

UTILS_SRCS := $(shell find src/utils/ -name "*.cc")
UTILS_OBJS := $(sort $(addprefix $(BUILD_DIR)/, $(UTILS_SRCS:.cc=.o)))

PERFMON_SRCS := src/perfmon.cc
PERFMON_OBJS:= $(sort $(addprefix $(BUILD_DIR)/, $(PERFMON_SRCS:.cc=.o)))

DAEMON_SRCS:= src/daemon.cc
DAEMON_OBJS:= $(sort $(addprefix $(BUILD_DIR)/, $(DAEMON_SRCS:.cc=.o)))

TEST_SRCS := src/test/pm_test.cc
TEST_OBJS:= $(sort $(addprefix $(BUILD_DIR)/, $(TEST_SRCS:.cc=.o)))


OBJS := $(sort $(MONITOR_OBJS) $(UTILS_OBJS) $(PERFMON_OBJS) $(DAEMON_OBJS) $(TEST_OBJS))

########################Compilation Section###################################

.PHONY: all 
all : $(TARGETS)

pm_monitor: $(MONITOR_OBJS) $(UTILS_OBJS) $(PERFMON_OBJS)
	$(CC) $^ -o $(BUILD_DIR)/$@ $(CXXFLAGS) $(LDFLAGS)
	@echo

pm_daemon: $(UTILS_OBJS) $(DAEMON_OBJS)
	$(CC) $^ -o $(BUILD_DIR)/$@ $(CXXFLAGS) $(LDFLAGS)
	@echo

pm_test: $(TEST_OBJS)
	$(CC) $^ -o $(BUILD_DIR)/$@ $(CXXFLAGS) $(LDFLAGS)
	@echo

#compile all files
$(OBJS) : $(BUILD_DIR)/%.o : %.cc
	@mkdir -p $(dir $@)
	$(CC) $< $(CXXFLAGS) -MMD -c -o $@
	cp $(BUILD_DIR)/$*.d $(BUILD_DIR)/$*.P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
		-e '/^$$/ d' -e 's/$$/ :/' < $(BUILD_DIR)/$*.d >> $(BUILD_DIR)/$*.P; \
	rm -f $*.d

.PHONY: clean 
clean:
	rm -rf $(BUILD_DIR)
	@echo
