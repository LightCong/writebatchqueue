package writebatchqueue

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

func (b *ItemBatch) Append(item Item) {
	b.BatchList[b.BatchListIndex] = item
	b.BatchListIndex += 1
}
