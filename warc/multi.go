package warc

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

func newID() string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(time.Now().UnixNano()))
	return hex.EncodeToString(b[:])
}

type meteredWriter struct {
	io.WriteCloser
	bytes uint64
}

func (m *meteredWriter) Write(b []byte) (int, error) {
	n, err := m.WriteCloser.Write(b)
	if n > 0 {
		atomic.AddUint64(&m.bytes, uint64(n))
	}
	return n, err
}

func (m *meteredWriter) Bytes() uint64 {
	return atomic.LoadUint64(&m.bytes)
}

type bufferedWriter struct {
	*bufio.Writer
	io.Closer
}

func newBufferedWriter(w io.WriteCloser) *bufferedWriter {
	return &bufferedWriter{
		Writer: bufio.NewWriter(w),
		Closer: w,
	}
}

func (w *bufferedWriter) Close() error {
	if err := w.Writer.Flush(); err != nil {
		return err
	}
	return w.Closer.Close()
}

func openFile(path string) (*meteredWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &meteredWriter{WriteCloser: newBufferedWriter(f)}, nil
}

// Unsafe for concurrent access.
type multiWriter struct {
	pattern string
	maxSize uint64

	cur *meteredWriter
}

func newMultiWriter(pattern string, maxSize uint64) rawWriter {
	if maxSize == 0 {
		maxSize = 100 * 1024 * 1024
	}
	return &multiWriter{
		pattern: pattern,
		maxSize: maxSize,
	}
}

func (w *multiWriter) newFilename() string {
	return fmt.Sprintf(w.pattern, newID())
}

func (w *multiWriter) NewRecord() (err error) {
	if w.cur == nil || w.cur.Bytes() > w.maxSize {
		if w.cur != nil {
			if err = w.cur.Close(); err != nil {
				return
			}
		}
		w.cur, err = openFile(w.newFilename())
	}
	return
}

func (w *multiWriter) Write(b []byte) (int, error) {
	return w.cur.Write(b)
}

func (w *multiWriter) Close() error {
	return w.cur.Close()
}

type simpleWriter struct {
	*bufferedWriter
}

func newSimpleWriter(w io.WriteCloser) rawWriter {
	return &simpleWriter{newBufferedWriter(w)}
}

func (w *simpleWriter) NewRecord() error {
	return nil
}

type rawWriter interface {
	io.WriteCloser
	NewRecord() error
}
