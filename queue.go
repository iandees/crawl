package crawl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

type queue struct {
	db        *gobDB
	numActive int32
}

var (
	queuePrefix  = []byte("queue")
	activePrefix = []byte("queue_active")

	queueKeySep   = []byte{'/'}
	queueKeySepP1 = []byte{'/' + 1}
)

type queuePair struct {
	key []byte

	URL   string
	Depth int
}

// Scan the pending queue and send items on 'ch'. Returns an error
// when the queue is empty (work is done).
func (q *queue) Scan(ch chan<- queuePair) error {
	n := 0
	startKey, endKey := queueScanRange()
	iter := q.db.NewRangeIterator(startKey, endKey)
	defer iter.Release()

	for iter.Next() {
		var p queuePair
		if err := iter.Value(&p); err != nil {
			continue
		}
		p.key = iter.Key()
		q.acquire(p)
		ch <- p
		n++
	}

	if n == 0 && q.numActive == 0 {
		return errors.New("EOF")
	}
	return nil
}

// Add an item to the pending work queue.
func (q *queue) Add(wb *leveldb.Batch, urlStr string, depth int, when time.Time) {
	t := uint64(when.UnixNano())
	qkey := bytes.Join([][]byte{queuePrefix, encodeUint64(t), encodeUint64(uint64(rand.Int63()))}, queueKeySep)
	q.db.PutObjBatch(wb, qkey, &queuePair{URL: urlStr, Depth: depth})
}

func (q *queue) acquire(qp queuePair) {
	wb := new(leveldb.Batch)
	q.db.PutObjBatch(wb, activeQueueKey(qp.key), qp)
	wb.Delete(qp.key)
	q.db.Write(wb, nil)

	atomic.AddInt32(&q.numActive, 1)
}

// Release an item from the queue. Processing for this item is done.
func (q *queue) Release(wb *leveldb.Batch, qp queuePair) {
	wb.Delete(activeQueueKey(qp.key))
	atomic.AddInt32(&q.numActive, -1)
}

// Retry processing this item at a later time.
func (q *queue) Retry(wb *leveldb.Batch, qp queuePair, delay time.Duration) {
	wb.Delete(activeQueueKey(qp.key))
	q.Add(wb, qp.URL, qp.Depth, time.Now().Add(delay))
	atomic.AddInt32(&q.numActive, -1)
}

// Recover moves all active tasks to the pending queue. To be
// called at startup to recover tasks that were active when the
// previous run terminated.
func (q *queue) Recover() {
	wb := new(leveldb.Batch)

	prefix := bytes.Join([][]byte{activePrefix, []byte{}}, queueKeySep)
	iter := q.db.NewPrefixIterator(prefix)
	defer iter.Release()
	for iter.Next() {
		var p queuePair
		if err := iter.Value(&p); err != nil {
			continue
		}
		p.key = iter.Key()[len(activePrefix)+1:]
		q.db.PutObjBatch(wb, p.key, &p)
		wb.Delete(iter.Key())
	}

	q.db.Write(wb, nil)
}

func encodeUint64(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func activeQueueKey(key []byte) []byte {
	return bytes.Join([][]byte{activePrefix, key}, queueKeySep)
}

func queueScanRange() ([]byte, []byte) {
	tlim := uint64(time.Now().UnixNano() + 1)
	startKey := bytes.Join([][]byte{queuePrefix, []byte{}}, queueKeySep)
	endKey := bytes.Join([][]byte{queuePrefix, encodeUint64(tlim)}, queueKeySep)
	return startKey, endKey
}
