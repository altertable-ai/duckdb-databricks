PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=databricks
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test-fake smoke
test-fake:
	$(PROJ_DIR)scripts/test_with_fake.sh

smoke:
	$(PROJ_DIR)scripts/test_with_databricks.sh