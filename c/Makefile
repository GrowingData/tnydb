
C_SRCS += \
tny.c \
tny_list.c \
tny_map.c \
tny_mem.c \
tny_page.c 

REL_OBJS += \
bin/release/tny.o \
bin/release/tny_list.o \
bin/release/tny_map.o \
bin/release/tny_mem.o \
bin/release/tny_page.o  


DBG_OBJS += \
bin/debug/tny.o \
bin/debug/tny_list.o \
bin/debug/tny_map.o \
bin/debug/tny_mem.o \
bin/debug/tny_page.o  


C_OPTS = -Wall -c -fmessage-length=0 -msse4.1 -std=gnu99 -fPIC -MMD -MP


bin/release/%.o: %.c
	gcc -O3 $(C_OPTS)  -o "$@" "$<"

bin/debug/%o: %c
	gcc -O0 -g3 $(C_OPTS)  -o "$@" "$<"

release: $(REL_OBJS)
	gcc -shared -o "bin/release/libtnydb.so" $(REL_OBJS) $(LIBS)

debug: $(DBG_OBJS)
	gcc -shared -o "bin/debug/libtnydb.so" $(DBG_OBJS) $(LIBS)
	