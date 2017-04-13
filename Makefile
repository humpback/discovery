#
#  humpback discovery Makefile 
#

export GOPATH=D:/work/projects/golibs

GO_CMD=go
GO_BUILD=$(GO_CMD) build
GO_BUILD_RACE=$(GO_CMD) build -race
GO_TEST=$(GO_CMD) test
GO_TEST_VERBOSE=$(GO_CMD) test -v
GO_INSTALL=$(GO_CMD) install -v
GO_CLEAN=$(GO_CMD) clean
GO_DEPS=$(GO_CMD) get -d -v
GO_DEPS_UPDATE=$(GO_CMD) get -d -v -u
GO_VET=$(GO_CMD) vet
GO_FMT=$(GO_CMD) fmt
GO_LINT=golint

# Packages
TOP_PACKAGE_DIR := ./
PACKAGE_LIST := backends backends/kv

.PHONY: all build build-race test test-verbose deps update-deps install clean fmt vet lint

all: build

build: vet
	@for p in $(PACKAGE_LIST); do \
		echo "==> Build $$p ..."; \
		$(GO_BUILD) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

build-race: vet
	@for p in $(PACKAGE_LIST); do \
		echo "==> Build $$p ..."; \
		$(GO_BUILD_RACE) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

test: deps
	@for p in $(PACKAGE_LIST); do \
		echo "==> Unit Testing $$p ..."; \
		$(GO_TEST) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

test-verbose: deps
	@for p in $(PACKAGE_LIST); do \
		echo "==> Unit Testing $$p ..."; \
		$(GO_TEST_VERBOSE) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

deps:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Install dependencies for $$p ..."; \
		$(GO_DEPS) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

update-deps:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Update dependencies for $$p ..."; \
		$(GO_DEPS_UPDATE) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

install:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Install $$p ..."; \
		$(GO_INSTALL) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done

clean:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Clean $$p ..."; \
		$(GO_CLEAN) $(TOP_PACKAGE_DIR)/$$p; \
	done

fmt:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Formatting $$p ..."; \
		$(GO_FMT) $(TOP_PACKAGE_DIR)/$$p || exit 1; \
	done
vet:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Vet $$p ..."; \
		$(GO_VET) $(TOP_PACKAGE_DIR)/$$p; \
	done

lint:
	@for p in $(PACKAGE_LIST); do \
		echo "==> Lint $$p ..."; \
		$(GO_LINT) src/$(TOP_PACKAGE_DIR)/$$p; \
	done