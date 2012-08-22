

PATH := $(PATH):
C_SRC_PATH:=$(CURDIR)/c


include c/Makefile

# Make sure that we have the latest build of the libtnydb
# and that its placed in the correct location
libtnydb: release-libtnydb
	cp $(CURDIR)/c/bin/release/libtnydb.so $(CURDIR)/golang/lib/


# Set up the paths required
path: 
	export GOPATH=$(GOPATH):$(CURDIR)/golang/
	export LD_LIBRARY_PATH=$(LD_LIBRARY_PATH):$(CURDIR)/golang/lib/


# Format all my code please
fmt:
	go fmt golang/src/tnydb/*.go


install: path libtnydb fmt
	go install tnydb

# Make the client executable
tnyc: install 
	go build $(CURDIR)/golang/src/tnyc.go 

# Make the server executable
tnyd: install 
	go build $(CURDIR)/golang/src/tnyd.go


clean: clean-c
	-$(RM) $(CURDIR)/golang/lib/libtnydb.so
	-$(RM) $(CURDIR)/tnyc
	-$(RM) $(CURDIR)/tnyd

# Make it all yo!
all: tnyd tnyc

