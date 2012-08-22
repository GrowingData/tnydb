package tnydb

import "bufio"
import "strings"
import "path/filepath"
import "os"

type io_fs_handle struct {
	writer *bufio.Writer
	reader *bufio.Reader
	file   *os.File
}

type TnyIO_FileSystem struct {
	handles map[string]io_fs_handle
}

func NewTnyIOFileSystem() TnyIO_FileSystem {
	var fs TnyIO_FileSystem
	fs.handles = make(map[string]io_fs_handle)
	return fs
}

func GetPath(path string) string {
	newPath := "data/" + strings.Replace(path, "filesystem://", "", 1)

	// Find the directory component so that we can make sure that 
	// all the sub directories actually exist
	dirPath := filepath.Dir(newPath)

	os.MkdirAll(dirPath, os.FileMode(0777))

	return newPath
}

func (self TnyIO_FileSystem) GetWriter(path string) *bufio.Writer {
	newPath := GetPath(path)
	fo, err := os.Create(newPath)
	if err != nil {
		panic(err)
	}

	var handle io_fs_handle
	handle.file = fo
	handle.writer = bufio.NewWriter(fo)

	self.handles[path] = handle

	return handle.writer
}

func (self TnyIO_FileSystem) GetReader(path string) (*bufio.Reader, error) {
	newPath := GetPath(path)
	fo, err := os.Open(newPath)
	if err != nil {
		return nil, err
	}

	var handle io_fs_handle
	handle.file = fo
	handle.reader = bufio.NewReader(fo)

	self.handles[path] = handle

	return handle.reader, nil
}

func (self TnyIO_FileSystem) Close(path string) {
	handle := self.handles[path]

	if handle.writer != nil {
		handle.writer.Flush()
	}
	if handle.reader != nil {

	}

	if handle.file != nil {
		handle.file.Close()
	}
}
