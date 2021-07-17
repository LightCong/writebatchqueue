package writebatchqueue

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	QueueStatus_Free    = 0
	QueueStatus_Running = 1
)

type StorageIf interface {
	CommitWriteBatch(batch *ItemBatch)
}

type WriteBatchQueue struct {
	//tasks 数组
	BatchQueue   list.List // guard by sync.Mutex
	QueueStatus  int       // guard by sync.Mutex
	ItemTotalNum int       // guard by sync.Mutex
	Mutex        sync.Mutex
	ItemNumLimit int
	BatchSize    int
	Storage      StorageIf
}

//enqueue 需要mutex 保护
func (wbq *WriteBatchQueue) pushback(item Item) {
	newItemBatch := false
	if wbq.BatchQueue.Len() == 0 {
		newItemBatch = true
	} else {
		lastItemBatch := wbq.BatchQueue.Back().Value.(*ItemBatch)
		if lastItemBatch.Size() == wbq.BatchSize {
			newItemBatch = true
		}
	}
	if newItemBatch {
		// create newItemBatch
		itemBatch := NewItemBatch(wbq.BatchSize)
		wbq.BatchQueue.PushBack(itemBatch)
	}
	lastBatch := wbq.BatchQueue.Back().Value.(*ItemBatch)
	lastBatch.Append(item)
	wbq.ItemTotalNum += 1
}

//popBatch 需要mutex 保护
func (wbq *WriteBatchQueue) popBatch() *ItemBatch {
	if wbq.BatchQueue.Len() == 0 {
		return nil
	}
	ele := wbq.BatchQueue.Front()
	firstBatch := wbq.BatchQueue.Front().Value.(*ItemBatch)
	wbq.BatchQueue.Remove(ele)
	wbq.ItemTotalNum -= firstBatch.Size()
	return firstBatch
}

func (wbq *WriteBatchQueue) Enqueue(item Item) error {
	//如果队列状态是runing 则任务入队并返回
	wbq.Mutex.Lock()
	if wbq.QueueStatus == QueueStatus_Running {
		if wbq.ItemTotalNum > wbq.ItemNumLimit {
			return fmt.Errorf("tasks too many  limit is %v", wbq.ItemNumLimit)
		}
		wbq.pushback(item)
		return nil
	}
	//当前队列状态为free,那么设置一下队列状态
	wbq.QueueStatus = QueueStatus_Running
	wbq.Mutex.Unlock()

	//同步执行任务
	itemBatch := NewItemBatch(1)
	itemBatch.Append(item)
	wbq.commitBatch(itemBatch)

	//判断队列是否为空 维护一下队列状态。
	//如果队列中有任务，则异步执行, 执行的过程中，新任务进来会入队
	if wbq.moreTasksOrChangeStatus() {
		go wbq.Execute()
	}
	return nil
}

func (wbq *WriteBatchQueue) moreTasksOrChangeStatus() bool {
	wbq.Mutex.Lock()
	defer wbq.Mutex.Unlock()

	if wbq.BatchQueue.Len() == 0 {
		wbq.QueueStatus = QueueStatus_Free
		return false
	}
	return true
}

func (wbq *WriteBatchQueue) commitBatch(batch *ItemBatch) {
	wbq.Storage.CommitWriteBatch(batch)
}

func (wbq *WriteBatchQueue) Execute() {
	for {
		wbq.Mutex.Lock()
		batch := wbq.popBatch()
		wbq.Mutex.Unlock()

		if batch != nil {
			wbq.commitBatch(batch)
		}

		if wbq.moreTasksOrChangeStatus() == false {
			break
		}
	}
}
