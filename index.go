package dump

import (
	"bytes"
	"github.com/AndreasBriese/bbloom"
	"github.com/djherbis/atime"
	"github.com/labstack/gommon/log"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type leaf struct {
	r          sync.Mutex
	a          sync.Mutex
	segment    Segment
	lastAccess int64
}

type IndexOpts struct {
	maxIdle              int64
	Dir                  string
	IndexCleanupInterval time.Duration
	MaxSegmentIdlePeriod time.Duration
	TimeToCompaction     time.Duration
	Logger               *log.Logger
	BloomfilterSize      float64
	BloomError           float64
}

type index struct {
	sync.Mutex
	opts  *IndexOpts
	bf    *bbloom.Bloom
	leafs map[string]*leaf
}

func (o *IndexOpts) setDefaults() {
	if o.Dir == "" {
		o.Dir = "."
	}
	if o.Dir[len(o.Dir)-1] != '/' {
		o.Dir += "/"
	}
	if o.MaxSegmentIdlePeriod == 0 {
		o.maxIdle = 5 * int64(time.Minute.Seconds())
	} else {
		o.maxIdle = int64(o.MaxSegmentIdlePeriod.Seconds())
	}
	if o.IndexCleanupInterval == 0 {
		o.IndexCleanupInterval = time.Minute
	}
	if o.BloomfilterSize == 0 {
		o.BloomfilterSize = 1 << 16 //65k items
	}
	if o.BloomError == 0 {
		o.BloomError = 0.01
	}
	if o.Logger == nil {
		o.Logger = log.New("index")
		o.Logger.SetLevel(log.DEBUG)
	}
	if o.TimeToCompaction == 0 {
		o.TimeToCompaction = time.Hour * (24*14 + 1) //compact segments that are at least 14 days old
	}
}

//  findAllSegments populates the bloom filter from list of files
//  Should only be run concurrently on one goroutine, and only assuming that
//  no writers are active.
func (i *index) findAllSegments() {
	i.opts.Logger.Infof("Searching for segments...")
	i.bf.Clear()
	gzFiles, err := filepath.Glob(i.opts.Dir + "*" + CompressedExt)
	if err != nil {
		panic(err) //Glob only returns errors if pattern is malformed
	}
	for _, f := range gzFiles {
		fName := f[len(i.opts.Dir) : len(f)-len(CompressedExt)] //return filename without path or extension
		i.bf.Add([]byte(fName))
	}
	datFiles, err := filepath.Glob(i.opts.Dir + "*" + NormalExt)
	if err != nil {
		panic(err) //Glob only returns errors if pattern is malformed
	}
	for _, f := range datFiles {
		fName := filepath.Base(f)
		fName = fName[0 : len(fName)-len(NormalExt)] //return filename without path or extension
		i.bf.Add([]byte(fName))
	}
	i.opts.Logger.Infof("Found %d segments", len(gzFiles)+len(datFiles))
}

func NewIndex(opts *IndexOpts) *index {
	opts.setDefaults()
	os.MkdirAll(opts.Dir, os.ModePerm)
	bf := bbloom.New(opts.BloomfilterSize, opts.BloomError)

	i := &index{leafs: make(map[string]*leaf), opts: opts, bf: &bf}
	i.findAllSegments()
	go func() {
		for {
			<-time.After(i.opts.IndexCleanupInterval)
			i.opts.Logger.Infof("starting cleanup...")
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
	removed := 0
	for path, l := range i.leafs {
		lastAccessed := atomic.LoadInt64(&l.lastAccess)
		if lastAccessed+i.opts.maxIdle < tNow {
			l.segment.Close()
			delete(i.leafs, path)
			removed++
		}
	}
	i.opts.Logger.Infof("Removed %d segments", removed)
}

func (i *index) addLeaf(path string) (*leaf, error) {
	p := filepath.Join(i.opts.Dir, path)
	s, err := NewSegment(p)
	if err != nil {
		return nil, err
	}
	l := &leaf{
		segment: s,
	}
	i.Lock()
	i.leafs[path] = l
	i.Unlock()
	i.bf.Add([]byte(path))
	i.opts.Logger.Debugf("Added new leaf '%s'", path)
	return l, nil
}

// Write writes data to a segment.
func (i *index) Write(path string, data []byte) error {
	i.opts.Logger.Debugf("Attempted write for segment '%s'", path)
	var (
		l   *leaf
		ok  bool
		err error
	)
	l, ok = i.leafs[path]
	if !ok {
		l, err = i.addLeaf(path)
		if err != nil {
			return err
		}
	}
	atomic.StoreInt64(&l.lastAccess, time.Now().Unix())
	i.opts.Logger.Debugf("Updated last access time for segment '%s'", path)
	l.a.Lock()
	defer l.a.Unlock()
	return l.segment.Append(data)
}

// Read applies a func to the file. Caller should not retain reference to io.Reader.
func (i *index) Read(path string, f func(io.Reader)) error {
	i.opts.Logger.Debugf("Attempted read for segment '%s'", path)
	if !i.Exists(path) {
		//if segment doesn't exist and we can short-circuit, then do that
		var r = bytes.NewBufferString("")
		f(r)
		return nil
	}
	var (
		l   *leaf
		ok  bool
		err error
	)
	l, ok = i.leafs[path]
	if !ok {
		l, err = i.addLeaf(path)
		if err != nil {
			return err
		}
	}
	atomic.StoreInt64(&l.lastAccess, time.Now().Unix())
	i.opts.Logger.Debugf("Updated last access time for segment '%s'", path)
	l.r.Lock()
	err = l.segment.Read(f)
	l.r.Unlock()
	return err
}

// Exists returns true if the path is in the index or false otherwise.
// It can return false positives but not false negatives.
func (i *index) Exists(path string) bool {
	return i.bf.HasTS([]byte(path))
}

//  Compact compressed old segments. The last accessed property of a segment determines
//  whether it gets compacted property or not.
func (i *index) Compact() error {
	i.opts.Logger.Infof("Searching for segments to compact...")
	datFiles, err := filepath.Glob(i.opts.Dir + "*" + NormalExt)
	if err != nil {
		panic(err) //Glob only returns errors if pattern is malformed
	}
	for _, f := range datFiles {
		lastAccessed, err := atime.Stat(f)
		i.opts.Logger.Debugf("Found segment '%s' with last accessed time %v", f, lastAccessed.UTC())
		if err != nil {
			return err
		}
		if lastAccessed.After(time.Now().Add(-1 * i.opts.TimeToCompaction)) {
			continue //don't compact recently used segments
		}
		fName := filepath.Base(f)
		fName = fName[0 : len(fName)-len(NormalExt)] //return filename without path or extension
		i.opts.Logger.Debugf("Compacting segment '%s'", fName)
		i.Lock()
		if i.leafs[fName] != nil {
			//very, very unlikely. Otherwise last access time would be more recent.
			i.opts.Logger.Warnf("Found compaction segment '%s' in cache but expected it to have been evicted", fName)
			err := i.leafs[fName].segment.Compress()
			if err != nil {
				i.Unlock()
				return err
			}
			delete(i.leafs, fName) //  after compaction segment won't be valid anymore
		} else {
			i.opts.Logger.Debugf("Allocating temporary segment for file '%s'", f)
			p := filepath.Join(i.opts.Dir, fName)
			s, err := NewSegment(p)
			if err != nil {
				return err
			}
			err = s.Compress()
			if err != nil {
				return err
			}
		}
		i.Unlock()
	}
	return nil
}
