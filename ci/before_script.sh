#! /bin/bash

# Godep & repo init
mkdir -p $GOPATH/src/$REPO_NAME
mv $CI_PROJECT_DIR/* $GOPATH/src/$REPO_NAME/
