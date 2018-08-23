#! /bin/bash

# Godep & repo init
go get -u github.com/golang/dep/cmd/dep
mkdir -p $GOPATH/src/$REPO_NAME
mv $CI_PROJECT_DIR/* $GOPATH/src/$REPO_NAME/
cd $GOPATH/src/$REPO_NAME
dep ensure