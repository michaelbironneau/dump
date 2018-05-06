package dump

import (
	"os"
	"log"
	"time"
	"sync"
	"io/ioutil"
	"github.com/francoispqt/gojay"
)

type Item struct {
	filename string
	Date string
	Time time.Time // = 1, required
	Value float64  // = 2, required
	Entity string // = 3, required
	Variable string // = 4, required
}

func (i *Item) MarshalObject(enc *gojay.Encoder){
	enc.AddFloatKey("entity", i.Value)
	enc.AddStringKey("time", i.Time.Format(time.RFC3339Nano))
	enc.AddStringKey("variable", i.Variable)
}

func (i *Item) IsNil() bool {
	return i == nil
}

func main(){
	fName := "test.log"

	fAppend, err := os.OpenFile(fName, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}


	wg := sync.WaitGroup{}
	wg.Add(2)
	go func(){
		for i := 0; i< 100; i++ {
			b, err := gojay.MarshalObject(&Item{
				Value: 1,
				Time: time.Now().UTC(),
				Entity: "oe1",
				Variable: "active-power",
			})
			if err != nil {
				log.Fatal(err)
			}
			fAppend.WriteString(string(b) + "\n")
			time.Sleep(time.Millisecond*100)
		}
		wg.Done()
	}()

	go func(){
		time.Sleep(time.Millisecond*1000*5)
		log.Printf("Done sleeping...trying to read file now.\n")
		b, err := ioutil.ReadFile(fName)
		if err != nil {
			log.Println(err)
		} else {
			log.Printf("Worked! Read %d characters.", len(b))
		}
		wg.Done()
	}()

	wg.Wait()


}
