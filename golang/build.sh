
export GOPATH=$GOPATH:/home/tez/Dropbox/Linux/eclipse/tnydb
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/tez/Dropbox/Linux/eclipse/tnydb/src

cp ../Release/libtnydb.so .
#FILES="test.go Page.go Column.go ColumnTypeInteger.go ColumnTypeString.go ValueContainer.go Table.go CsvReader.go ColumnTypeFloat64.go"
#go fmt $FILES
#go build -o tnydb $FILES 

clear

go fmt tnydb/*.go

go install tnydb
go build tnydb.go
go build tnyc.go