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

	queueKeySep = []byte{'/'}
)

type queuePair struct {
	key []byte

	URL   string
	Depth int
	Tag   int
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
		if err := q.acquire(p); err != nil {
			return err
		}
		ch <- p
		n++
	}

	if n == 0 && q.numActive == 0 {
		return errors.New("EOF")
	}
	return nil
}

// Add an item to the pending work queue.
func (q *queue) Add(wb *leveldb.Batch, urlStr string, tag, depth int, when time.Time) error {
	t := uint64(when.UnixNano())
	qkey := bytes.Join([][]byte{queuePrefix, encodeUint64(t), encodeUint64(uint64(rand.Int63()))}, queueKeySep)
	return q.db.PutObjBatch(wb, qkey, &queuePair{URL: urlStr, Tag: tag, Depth: depth})
}

func (q *queue) acquire(qp queuePair) error {
	wb := new(leveldb.Batch)
	if err := q.db.PutObjBatch(wb, activeQueueKey(qp.key), qp); err != nil {
		return err
	}
	wb.Delete(qp.key)
	if err := q.db.Write(wb, nil); err != nil {
		return err
	}

	atomic.AddInt32(&q.numActive, 1)
	return nil
}

// Release an item from the queue. Processing for this item is done.
func (q *queue) Release(wb *leveldb.Batch, qp queuePair) {
	wb.Delete(activeQueueKey(qp.key))
	atomic.AddInt32(&q.numActive, -1)
}

// Retry processing this item at a later time.
func (q *queue) Retry(wb *leveldb.Batch, qp queuePair, delay time.Duration) error {
	wb.Delete(activeQueueKey(qp.key))
	if err := q.Add(wb, qp.URL, qp.Tag, qp.Depth, time.Now().Add(delay)); err != nil {
		return err
	}
	atomic.AddInt32(&q.numActive, -1)
	return nil
}

// Recover moves all active tasks to the pending queue. To be
// called at startup to recover tasks that were active when the
// previous run terminated.
func (q *queue) Recover() error {
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
		if err := q.db.PutObjBatch(wb, p.key, &p); err != nil {
			return err
		}
		wb.Delete(iter.Key())
	}

	return q.db.Write(wb, nil)
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
