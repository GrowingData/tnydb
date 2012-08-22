
ifeq ($(C_SRC_PATH),)
	C_SRC_PATH:=$(CURDIR)
endif

C_SRCS += \
$(C_SRC_PATH)/tny.c \
$(C_SRC_PATH)/tny_list.c \
$(C_SRC_PATH)/tny_map.c \
$(C_SRC_PATH)/tny_mem.c \
$(C_SRC_PATH)/tny_page.c 

REL_OBJS += \
$(C_SRC_PATH)/bin/release/tny.o \
$(C_SRC_PATH)/bin/release/tny_list.o \
$(C_SRC_PATH)/bin/release/tny_map.o \
$(C_SRC_PATH)/bin/release/tny_mem.o \
$(C_SRC_PATH)/bin/release/tny_page.o  


DBG_OBJS += \
$(C_SRC_PATH)/bin/debug/tny.o \
$(C_SRC_PATH)/bin/debug/tny_list.o \
$(C_SRC_PATH)/bin/debug/tny_map.o \
$(C_SRC_PATH)/bin/debug/tny_mem.o \
$(C_SRC_PATH)/bin/debug/tny_page.o  


C_OPTS = -Wall -c -fmessage-length=0 -msse4.1 -std=gnu99 -fPIC -MMD -MP


$(C_SRC_PATH)/bin/release/%.o: $(C_SRC_PATH)/%.c
	gcc -O3 $(C_OPTS) -o "$@" "$<"

$(C_SRC_PATH)/bin/debug/%.o: $(C_SRC_PATH)/%.c
	gcc -O0 -g3 $(C_OPTS) -o "$@" "$<"

release-libtnydb: $(REL_OBJS)
	gcc -shared -o "$(C_SRC_PATH)/bin/release/libtnydb.so" $(REL_OBJS) $(LIBS)

debug-libtnydb: $(DBG_OBJS)
	gcc -shared -o "$(C_SRC_PATH)/bin/debug/libtnydb.so" $(DBG_OBJS) $(LIBS)

all: release-libtnydb debug-libtnydb
	


clean-c:
	-$(RM) $(C_SRC_PATH)/bin/release/*
	-$(RM) $(C_SRC_PATH)/bin/debug/*

clean: clean-c

test:
	echo $(C_SRC_PATH)/bin/release/tny.o