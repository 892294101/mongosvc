GOCMD=go
#export TAG=1.1.0
#export BD=$(shell date '+%b %d %Y %T')
#GOBUILD=${GOCMD} build -gcflags=all='-l -N' -ldflags "-s -w -X 'github.com/892294101/mongosvc/mparallel/src/auth.Version=$(TAG)' -X 'github.com/892294101/mongosvc/mparallel/src/auth.BDate=$(BD)'"
GOBUILD=${GOCMD} build -gcflags=all='-l -N' -ldflags "-s -w"

BUILD_DIR=./build
BINARY_DIR=$(BUILD_DIR)/bin

MYSQL_EXTRACT_FILE=$(BINARY_DIR)/mongosvc
MYSQL_EXTRACT_SRC=./src/main.go


.PHONY: all clean build

all: clean build

clean:

build:
	${GOBUILD} -o ${MYSQL_EXTRACT_FILE} ${MYSQL_EXTRACT_SRC}

