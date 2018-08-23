#! /bin/bash
PKG_LIST=$(go list ./... | grep -v /vendor/)
mkdir -p coverage
rm -f coverage/*.cov
for package in ${PKG_LIST}; do
	go test -covermode=count -coverprofile "coverage/${package##*/}.cov" "$package"
done
echo 'mode: count' > ./system.cov
tail -q -n +2 ./coverage/*.cov >> ./system.cov

go tool cover -func=system.cov
mkdir -p ${CI_PROJECT_DIR}/artifacts
go tool cover -html=system.cov -o ${CI_PROJECT_DIR}/artifacts/coverage.html
