package writebatchqueue

import (
	"fmt"
	"strconv"
	"testing"
)

type MockItem struct {
	Num int
}

func (i *MockItem) Serialize() (string, error) {
	return strconv.Itoa(i.Num), nil
}

func TestItemBatch_Append(t *testing.T) {
	b := NewItemBatch(10)
	for i := 0; i < 10; i++ {
		mockItem := &MockItem{}
		b.Append(mockItem)
		fmt.Println(b.BatchListIndex, b.BatchList)
	}
	mockItem := &MockItem{}
	err := b.Append(mockItem)
	fmt.Println(err)
}
