package dump

import (
	"io"
	"os"
	"path/filepath"
	"compress/gzip"
	"errors"
)

const (
	CompressedExt = ".gz"
	NormalExt = ".dat"
)

var (
	ErrImmutableSegment = errors.New("the segment has been compressed and is now immutable")
)
// extension returns the desired extension for new segment. If already compressed
// it returns true (exists), the filename with ext, and no error. Otherwise it
// returns false (don't know if exists), filename with ext, and error (maybe).
func extension(path string) (bool, string, bool, error) {
	if f, err := os.Stat(path + CompressedExt); err == nil && f.IsDir() == false {
		return true, path + CompressedExt, true, nil
	} else if os.IsNotExist(err) {
		return false, path + NormalExt, false, nil
	} else {
		return false, "", false, err
	}
}

func NewSegment(path string) (Segment, error) {
	exists, filename, compressed, err := extension(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		err := os.MkdirAll(filepath.Dir(filename), os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	fAppend, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	fRead, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	var cReader *gzip.Reader
	if compressed {
		cReader, err = gzip.NewReader(fRead)
		if err != nil {
			return nil, err
		}
	}
	return &segment {
		path: path,
		filename: filename,
		appender: fAppend,
		rFile:    fRead,
		gReader: cReader,
	}, nil
}

//  Segment is essentially an append-only file. Not safe for concurrent use by
//  multiple goroutines.
type Segment interface{
	Append([]byte) error
	Read(func(io.Reader)) error
	Compress() error
	Close()
}

type segment struct {
	path string
	filename string
	appender *os.File
	rFile    *os.File
	gReader  *gzip.Reader
}

// Compress compresses the file and deletes the original (uncompressed) version.
// References to the segment should be discarded and it should be recreated next
// time it is needed.
// Warning: A segment that has been compressed is immutable thereafter.
func (s *segment) Compress() error {
	s.appender.Close()
	if s.gReader != nil {
		s.gReader.Close()
	}
	f, err := os.OpenFile(s.path + CompressedExt, os.O_CREATE | os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	gWriter := gzip.NewWriter(f)
	_, err = s.rFile.Seek(0,0)
	if err != nil {
		return err
	}
	_, err = io.Copy(gWriter, s.rFile)
	if err != nil {
		return err
	}
	err = gWriter.Flush()
	if err != nil {
		return err
	}
	gWriter.Close()
	return nil
}

//  Append appends the given content to the segment. It should
//  be called relatively few times (not every single item) as it
//  always flushes the underlying OS buffer before returning.
func (s *segment) Append(b []byte) error {
	if s.gReader != nil {
		return ErrImmutableSegment
	}
	_, err := s.appender.Write(b)
	if err != nil {
		return err
	}
	return s.appender.Sync()
}

//  Reader returns a read-only handle to the underlying file that has
//  been Seek'ed back to offset 0.
func (s *segment) Read(f func(io.Reader)) error {
	var err error
	if s.gReader != nil {
		f(s.gReader)
	} else {
		f(s.rFile)
	}
	_, err = s.rFile.Seek(0,0)
	if err != nil {
		return err
	}
	if s.gReader != nil {
		err = s.gReader.Reset(s.rFile)
	}
	return err
}

// Close is a best effort attempt to close the underlying files
func (s *segment) Close() {
	s.rFile.Close()
	s.appender.Close()
	if s.gReader != nil {
		s.gReader.Close()
	}
}