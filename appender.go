package main

import (
	"time"
	"os"
	"sort"
	"github.com/francoispqt/gojay"
)

const separator = "__"

type ItemSlice []Item

func (is ItemSlice) Len() int {return len(is)}
func (is ItemSlice) Swap(i, j int){is[i], is[j] = is[j], is[i]}
func (is ItemSlice) Less(i,j int) bool {
	return is[i].filename < is[j].filename
}

type Manager interface {
	//Ingest stores the given items
	Ingest(items []Item) error

	//Read reads the entire files containing items for the given start/finish
	Read(entity, variable string, start, finish time.Time) ([]Item, error)
}

type appender struct {
	dir string
	handles map[string]*os.File

}



func (a *appender) PopulateFilename(item *Item){
	item.filename = item.Entity + separator + item.Variable + separator + item.Date + ".log"
}

func (a *appender) Ingest(items ItemSlice) error {
	sort.Sort(items) //sort items by destination filename to help performance (less contention)
	for i := range items {
		if err := a.ingestItem(&items[i]); err != nil {
			return err
		}
	}
	return nil
}

func (a *appender) getHandle(filename string) (*os.File, error){
	h, ok := a.handles[filename]
	if ok {
		return h, nil
	}
	f, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600)

	if err != nil {
		return nil, err
	}
	a.handles[filename] = f
	return f, nil
}


func (a *appender) ingestItem(item *Item) error {
	f, err := a.getHandle(item.filename)
	if err != nil {
		return err
	}
	b, err := gojay.MarshalObject(item)
	if err != nil {
		return err
	}
	f.WriteString(string(b) + "\n")
	return nil
}
