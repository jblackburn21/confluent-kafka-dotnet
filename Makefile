# This is a maintainer-centric Makefile to help performing
# typical operations for all projects in this repo.
#
# Example:
#   make build

OS=$(shell uname -s)

EXAMPLE_DIRS=$(shell find ./examples -name '*.csproj' -exec dirname {} \;)
TEST_DIRS=$(filter-out %StrongName.UnitTests/,$(filter %UnitTests/,$(dir $(wildcard test/**/*.csproj))))
MAIN_DIRS=$(dir $(wildcard src/**/*.csproj))

LINUX_FRAMEWORK=netcoreapp2.0
DEFAULT_FRAMEWORK?=$(LINUX_FRAMEWORK)

all:
	@echo "Usage:   make <dotnet-command>"
	@echo "Example: make build - runs 'dotnet build' for all projects"

.PHONY: test

build:
	# Assuming .NET Core on Linux (net451 will not work).
	@set -e && for d in $(MAIN_DIRS); do \
		echo '*****' Running build in $$d '*****' ; \
		dotnet $@ $$d; \
	done

test:
	@set -e && for d in $(TEST_DIRS); do \
		echo '*****' Running Tests in $$d '*****' ; \
		dotnet $@ $$d; \
	done

pack:
	for d in $(MAIN_DIRS) ; do dotnet $@ $$d; done ;
