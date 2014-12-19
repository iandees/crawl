package crawl

import (
	"fmt"
	"io"
	"time"

	"compress/gzip"

	"code.google.com/p/go-uuid/uuid"
)

var (
	warcTimeFmt      = time.RFC3339
	warcVersion      = "WARC/1.0"
	warcContentTypes = map[string]string{
		"warcinfo": "application/warc-fields",
		"response": "application/http; msgtype=response",
		"request":  "application/http; msgtype=request",
		"metadata": "application/warc-fields",
	}
)

// A Warc header. Header field names are case-sensitive.
type WarcHeader map[string]string

// Set a header to the specified value. Multiple values are not
// supported.
func (h WarcHeader) Set(key, value string) {
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
func (h WarcHeader) Get(key string) string {
	return h[key]
}

// Encode the header to a Writer.
func (h WarcHeader) Encode(w io.Writer) {
	fmt.Fprintf(w, "%s\r\n", warcVersion)
	for hdr, value := range h {
		fmt.Fprintf(w, "%s: %s\r\n", hdr, value)
	}
	fmt.Fprintf(w, "\r\n")
}

// NewWarcHeader returns a WarcHeader with its own unique ID and the
// current timestamp.
func NewWarcHeader() WarcHeader {
	h := make(WarcHeader)
	h.Set("WARC-Record-ID", fmt.Sprintf("<%s>", uuid.NewUUID().URN()))
	h.Set("WARC-Date", time.Now().Format(warcTimeFmt))
	h.Set("Content-Type", "application/octet-stream")
	return h
}

// WarcWriter can write records to a file in WARC format.
type WarcWriter struct {
	writer io.WriteCloser
}

type recordWriter struct {
	io.Writer
}

func (rw *recordWriter) Close() error {
	// Add the end-of-record marker.
	fmt.Fprintf(rw, "\r\n\r\n")
	return nil
}

// NewRecord starts a new WARC record with the provided header. The
// caller must call Close on the returned writer before creating the
// next record.
func (w *WarcWriter) NewRecord(hdr WarcHeader) io.WriteCloser {
	hdr.Encode(w.writer)
	return &recordWriter{w.writer}
}

// Close the WARC writer and flush all buffers.
func (w *WarcWriter) Close() error {
	return w.writer.Close()
}

func NewWarcWriter(w io.WriteCloser) *WarcWriter {
	return &WarcWriter{gzip.NewWriter(w)}
}
