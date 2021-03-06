package tnydb

import "bufio"

type TnyIO interface {
	GetWriter(path string) *bufio.Writer
	GetReader(path string) (*bufio.Reader, error)

	Close(path string)
}
