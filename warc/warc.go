// Package to write WARC files.

package warc

import (
	"fmt"
	"io"
	"time"

	"compress/gzip"

	"code.google.com/p/go-uuid/uuid"
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

// A WARC header. Header field names are case-sensitive.
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
func (h Header) Encode(w io.Writer) {
	fmt.Fprintf(w, "%s\r\n", warcVersion)
	for hdr, value := range h {
		fmt.Fprintf(w, "%s: %s\r\n", hdr, value)
	}
	fmt.Fprintf(w, "\r\n")
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
	writer   io.WriteCloser
	gzwriter *gzip.Writer
	lockCh   chan bool
}

type recordWriter struct {
	io.Writer
	lockCh chan bool
}

func (rw *recordWriter) Close() error {
	// Add the end-of-record marker.
	fmt.Fprintf(rw, "\r\n\r\n")

	<-rw.lockCh

	return nil
}

// NewRecord starts a new WARC record with the provided header. The
// caller must call Close on the returned writer before creating the
// next record. Note that this function may block until that condition
// is satisfied.
func (w *Writer) NewRecord(hdr Header) io.WriteCloser {
	w.lockCh <- true
	if w.gzwriter != nil {
		w.gzwriter.Close()
	}
	w.gzwriter, _ = gzip.NewWriterLevel(w.writer, gzip.BestCompression)
	w.gzwriter.Header.Name = hdr.Get("WARC-Record-ID")
	hdr.Encode(w.gzwriter)
	return &recordWriter{Writer: w.gzwriter, lockCh: w.lockCh}
}

// Close the WARC writer and flush all buffers. This will also call
// Close on the wrapped io.WriteCloser object.
func (w *Writer) Close() error {
	if err := w.gzwriter.Close(); err != nil {
		return err
	}
	return w.writer.Close()
}

// NewWriter initializes a new Writer and returns it.
func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{
		writer: w,
		// Buffering is important here since we're using this
		// channel as a semaphore.
		lockCh: make(chan bool, 1),
	}
}
