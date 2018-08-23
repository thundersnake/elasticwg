REPO_NAME := "gitlab.com/thundersnake/elasticwg"
PKG_LIST := $(shell go list ${REPO_NAME}/... | grep -v /vendor/)

.PHONY: all dep doc build test

all: test lint doc build

lint: ## test
	@go get -u github.com/golang/lint/golint
	@${GOPATH}/bin/golint -set_exit_status ${PKG_LIST}

test: dep ## Run unittests
	@go test -short ${PKG_LIST}

junit: dep
	@go get -u github.com/jstemmer/go-junit-report
	@go test -v 2>&1 | ${GOPATH}/bin/go-junit-report > report.xml

race: dep ## Run data race detector
	@go test -race -short ${PKG_LIST}

msan: dep ## Run memory sanitizer
	@go test -msan -short ${PKG_LIST}

dep:
	@go get -u github.com/golang/dep/cmd/dep
	@${GOPATH}/bin/dep ensure
