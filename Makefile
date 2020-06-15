.PHONY: default
default: | help

.PHONY: install-build-tools
install-build-tools: ## Install required tools for build/dev
	pip install tox wheel twine bumpversion

.PHONY: build
build: ## Build dist
	python setup.py sdist bdist_wheel

.PHONY: test
test: ## Run tests
	tox

.PHONY: clean
clean: ## Clean all build artifacts
	rm -rf .tox
	rm -rf *.egg-info
	rm -rf dist

.PHONY: release-validate
release-validate: ## Validate that a distribution will render properly on PyPI
	@make clean build test
	twine check dist/*

.PHONY: release-test
release-test: ## Release a new version, uploading it to PyPI Test
	@make release-validate
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

.PHONY: release
release: ## Release a new version, uploading it to PyPI
	@make release-validate
	twine upload dist/*

.PHONY: bump-version-patch
bump-version-patch: ## Bump patch version, e.g. 0.0.1 -> 0.0.2
	bumpversion patch

.PHONY: bump-version-minor
bump-version-minor: ## Bump minor version, e.g. 0.0.1 -> 0.1.0
	bumpversion minor


.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
