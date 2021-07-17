package writebatchqueue

import "fmt"

type Item interface {
	Serialize() (string, error)
}

func NewItemBatch(batchsize int) *ItemBatch {
	return &ItemBatch{
		BatchList:      make([]Item, batchsize),
		BatchListIndex: 0,
	}
}

type ItemBatch struct {
	BatchList      []Item
	BatchListIndex int
}

func (b *ItemBatch) Append(item Item) error {
	if b.BatchListIndex == cap(b.BatchList) {
		return fmt.Errorf("batch is full")
	}
	b.BatchList[b.BatchListIndex] = item
	b.BatchListIndex += 1
	return nil
}

func (b *ItemBatch) Size() int {
	return len(b.BatchList)
}
