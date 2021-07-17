package writebatchqueue

import (
	"fmt"
	"testing"
)

type MockItem struct {
}

func (i *MockItem) Serialize() (string, error) {
	return "abc", nil
}

func TestItemBatch_Append(t *testing.T) {
	b := NewItemBatch(10)
	for i:=0;i<10;i++ {
		mockItem:=&MockItem{}
		b.Append(mockItem)
		fmt.Println(b.BatchListIndex,b.BatchList)
	}
	mockItem:=&MockItem{}
	err:=b.Append(mockItem)
	fmt.Println(err)
}
