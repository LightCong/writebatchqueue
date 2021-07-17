package writebatchqueue

import (
	"fmt"
	"testing"
	"time"
)

type MockStorage struct {
}

func (s *MockStorage) CommitWriteBatch(batch *ItemBatch) {
	lst := []string{}
	for _, item := range batch.BatchList {
		s, _ := item.Serialize()
		lst = append(lst, s)
	}
	fmt.Println(lst)
	time.Sleep(100 * time.Millisecond)
}


func TestWriteBatchQueue_Execute(t *testing.T) {
	
}

func TestWriteBatchQueue_Enqueue(t *testing.T) {

}