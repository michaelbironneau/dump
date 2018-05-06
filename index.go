package main

import (
	"sync"
	"time"
	"io"
)

type leaf struct {
	r sync.Mutex
	a sync.Mutex
	segment Segment
	lastAccess int64
}

type indexOpts struct {
	maxIdle int64
	IndexCleanupInterval time.Duration
	MaxSegmentIdlePeriod time.Duration
}

type index struct {
	sync.Mutex
	opts *indexOpts
	leafs map[string]*leaf
}

func (o *indexOpts) setDefaults(){
	if o.MaxSegmentIdlePeriod == 0 {
		o.maxIdle = 5*time.Minute.Nanoseconds()
	}
	if o.IndexCleanupInterval == 0 {
		o.IndexCleanupInterval = time.Minute
	}
}

func NewIndex(opts *indexOpts) *index {
	opts.setDefaults()
	i := &index{leafs: make(map[string]*leaf), opts: opts}
	go func(){
		for {
			<- time.After(i.opts.IndexCleanupInterval)
			i.cleanup()
		}
	}()
	return i
}

// cleanup closes segments that have not been accessed for MaxSegmentIdlePeriod
func (i *index) cleanup() {
	i.Lock()
	defer i.Unlock()
	tNow := time.Now().Unix()
	for path, l := range i.leafs {
		if l.lastAccess + i.opts.maxIdle < tNow {
			l.a.Lock()
			l.r.Lock()
			l.segment.Close()
			delete(i.leafs, path)
		}
	}
}

func (i *index) addLeaf(path string) (*leaf, error) {
	s, err := NewSegment(path)
	if err != nil {
		return nil, err
	}
	l := &leaf{
		segment: s,
	}
	i.Lock()
	i.leafs[path] = l
	i.Unlock()
	return l, nil
}

// Write writes data to a segment.
func (i *index) Write(path string, data []byte) error {
	var (
		l *leaf
		ok bool
		err error
	)
	l, ok = i.leafs[path]
	if !ok {
		l, err = i.addLeaf(path)
		if err != nil {
			return err
		}
	}

	l.a.Lock()
	defer l.a.Unlock()
	return l.segment.Append(data)
}

// Read applies a func to the file. Caller should not retain reference to io.Reader.
func (i *index) Read(path string, f func(io.Reader)) error{
	var (
		l *leaf
		ok bool
		err error
	)
	l, ok = i.leafs[path]
	if !ok {
		l, err = i.addLeaf(path)
		if err != nil {
			return err
		}
	}

	l.r.Lock()
	err = l.segment.Read(f)
	defer l.r.Unlock()
	return err
}