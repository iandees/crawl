// Package to write WARC files.

package warc

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"compress/gzip"

	"github.com/pborman/uuid"
)

var (
	warcTimeFmt      = "2006-01-02T15:04:05Z"
	warcVersion      = "WARC/1.0"
	warcContentTypes = map[string]string{
		"warcinfo": "application/warc-fields",
		"response": "application/http; msgtype=response",
		"request":  "application/http; msgtype=request",
		"metadata": "application/warc-fields",
	}
)

// Header for a WARC record. Header field names are case-sensitive.
type Header map[string]string

// Set a header to the specified value. Multiple values are not
// supported.
func (h Header) Set(key, value string) {
	h[key] = value

	// Keep Content-Type in sync with WARC-Type.
	if key == "WARC-Type" {
		if ct, ok := warcContentTypes[value]; ok {
			h["Content-Type"] = ct
		} else {
			h["Content-Type"] = "application/octet-stream"
		}
	}
}

// Get the value of a header. If not found, returns an empty string.
func (h Header) Get(key string) string {
	return h[key]
}

// Encode the header to a Writer.
func (h Header) Encode(w io.Writer) error {
	if _, err := fmt.Fprintf(w, "%s\r\n", warcVersion); err != nil {
		return err
	}
	for hdr, value := range h {
		if _, err := fmt.Fprintf(w, "%s: %s\r\n", hdr, value); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

// NewHeader returns a Header with its own unique ID and the
// current timestamp.
func NewHeader() Header {
	h := make(Header)
	h.Set("WARC-Record-ID", fmt.Sprintf("<%s>", uuid.NewUUID().URN()))
	h.Set("WARC-Date", time.Now().UTC().Format(warcTimeFmt))
	h.Set("Content-Type", "application/octet-stream")
	return h
}

// Writer can write records to a file in WARC format. It is safe
// for concurrent access, since writes are serialized internally.
type Writer struct {
	writer rawWriter
	lockCh chan struct{}
}

type recordWriter struct {
	io.WriteCloser
	lockCh chan struct{}
}

func (rw *recordWriter) Close() error {
	// Add the end-of-record marker.
	_, err := io.WriteString(rw, "\r\n\r\n")
	if err != nil {
		return err
	}
	err = rw.WriteCloser.Close()
	<-rw.lockCh
	return err
}

// NewRecord starts a new WARC record with the provided header. The
// caller must call Close on the returned writer before creating the
// next record. Note that this function may block until that condition
// is satisfied. If this function returns an error, the state of the
// Writer is invalid and it should no longer be used.
func (w *Writer) NewRecord(hdr Header) (io.WriteCloser, error) {
	w.lockCh <- struct{}{}

	if err := w.writer.NewRecord(); err != nil {
		return nil, err
	}

	gzwriter, err := gzip.NewWriterLevel(w.writer, gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	gzwriter.Header.Name = hdr.Get("WARC-Record-ID")
	if err = hdr.Encode(gzwriter); err != nil {
		return nil, err
	}
	return &recordWriter{
		WriteCloser: gzwriter,
		lockCh:      w.lockCh,
	}, nil
}

// Close the WARC writer and flush all buffers. This will also call
// Close on the wrapped io.WriteCloser object.
func (w *Writer) Close() error {
	w.lockCh <- struct{}{} // do not release
	defer close(w.lockCh)  // pending NewRecord calls will panic?
	return w.writer.Close()
}

// NewWriter initializes a new Writer and returns it.
func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		writer: newSimpleWriter(w),
		// Buffering is important here since we're using this
		// channel as a semaphore.
		lockCh: make(chan struct{}, 1),
	}
}

// NewMultiWriter initializes a new Writer that writes its output to
// multiple files of limited size approximately equal to maxSize,
// rotating them when necessary. The input path should contain a
// literal '%s' token, which will be replaced with a (lexically
// sortable) unique token.
func NewMultiWriter(pattern string, maxSize uint64) (*Writer, error) {
	if !strings.Contains(pattern, "%s") {
		return nil, errors.New("input path is not a pattern")
	}
	return &Writer{
		writer: newMultiWriter(pattern, maxSize),
		// Buffering is important here since we're using this
		// channel as a semaphore.
		lockCh: make(chan struct{}, 1),
	}, nil
}
