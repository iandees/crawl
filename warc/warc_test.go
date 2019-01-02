package warc

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

var testData = []byte("this is some very interesting test data of non-zero size")

func writeRecords(w *Writer, n int) error {
	for i := 0; i < n; i++ {
		hdr := NewHeader()
		rec, err := w.NewRecord(hdr)
		if err != nil {
			return fmt.Errorf("NewRecord: %v", err)
		}
		_, err = rec.Write(testData)
		if err != nil {
			return fmt.Errorf("record Write: %v", err)
		}
		if err := rec.Close(); err != nil {
			return fmt.Errorf("record Close: %v", err)
		}
	}
	return nil
}

func writeManyRecords(t testing.TB, w *Writer, n int) {
	if err := writeRecords(w, n); err != nil {
		t.Fatal(err)
	}
}

func writeManyRecordsConcurrently(t testing.TB, w *Writer, n, nproc int) {
	startCh := make(chan struct{})
	errCh := make(chan error, nproc+1)
	var wg sync.WaitGroup

	for i := 0; i < nproc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			if err := writeRecords(w, n); err != nil {
				errCh <- err
			}
		}()
	}
	go func() {
		wg.Wait()
		errCh <- nil
	}()
	close(startCh)
	if err := <-errCh; err != nil {
		t.Fatalf("a worker got an error: %v", err)
	}
}

func TestWARC_WriteSingleFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(filepath.Join(dir, "out.warc.gz"))
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f)

	writeManyRecords(t, w, 1000)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWARC_WriteMulti(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var targetSize int64 = 10240
	w, err := NewMultiWriter(filepath.Join(dir, "out.%s.warc.gz"), uint64(targetSize))
	if err != nil {
		t.Fatal(err)
	}

	writeManyRecords(t, w, 1000)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files, _ := ioutil.ReadDir(dir)
	if len(files) < 2 {
		t.Fatalf("MultiWriter didn't create enough files (%d)", len(files))
	}
	for _, f := range files[:len(files)-1] {
		if f.Size() < targetSize {
			t.Errorf("output file %s is too small (%d bytes)", f.Name(), f.Size())
		}
	}
}

func TestWARC_WriteMulti_Concurrent(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	var targetSize int64 = 100000
	w, err := NewMultiWriter(filepath.Join(dir, "out.%s.warc.gz"), uint64(targetSize))
	if err != nil {
		t.Fatal(err)
	}

	writeManyRecordsConcurrently(t, w, 1000, 10)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	files, _ := ioutil.ReadDir(dir)
	if len(files) < 2 {
		t.Fatalf("MultiWriter didn't create enough files (%d)", len(files))
	}
	for _, f := range files[:len(files)-1] {
		if f.Size() < targetSize {
			t.Errorf("output file %s is too small (%d bytes)", f.Name(), f.Size())
		}
	}
}
