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
	BatchQueue list.List // guard by sync.Mutex
	QueueStatus    int       // guard by sync.Mutex
	ItemTotalNum   int       // guard by sync.Mutex
	Mutex          sync.Mutex
	ItemNumLimit  int
	BatchSize int
	Storage  StorageIf
}

//enqueue 需要mutex 保护
func (wbq *WriteBatchQueue) enqueue(item Item) {
	newItemBatch := false
	if wbq.BatchQueue.Len() == 0 {
		newItemBatch = true
	} else {
		lastItemBatch := wbq.BatchQueue.Back().Value.(*ItemBatch)
		if len(lastItemBatch.BatchList) == wbq.BatchSize {
			newItemBatch = true
		}
	}
	if newItemBatch {
		//create TaskBatch
		itemBatch := NewItemBatch(wbq.BatchSize)
		wbq.BatchQueue.PushBack(itemBatch)
	}
	lastBatch := wbq.BatchQueue.Back().Value.(*ItemBatch)
	lastBatch.Append(item)
	wbq.ItemNumLimit += 1
}

func (wbq *WriteBatchQueue) Enqueue(item Item) error {
	//如果队列状态是runing 则任务入队并返回
	wbq.Mutex.Lock()
	if wbq.QueueStatus == QueueStatus_Running {
		if wbq.ItemTotalNum > wbq.ItemNumLimit {
			return fmt.Errorf("tasks too many  limit is %v", wbq.ItemNumLimit)
		}
		wbq.enqueue(item)
		return nil
	}
	//当前队列状态为free,那么设置一下队列状态
	wbq.QueueStatus = QueueStatus_Running
	wbq.Mutex.Unlock()

	//todo 同步执行任务
	wbq.ExecTask(t)

	//判断队列是否为空 维护一下队列状态。
	//如果队列中有任务，则开一个异步bthread继续执行
	if wbq.moreTasksOrChangeStatus() {
		go wbq.ExecTasks()
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

func (wbq *WriteBatchQueue) ExecTask(batch *ItemBatch) error {
	wbq.Storage.CommitWriteBatch(batch)
	return nil
}

func (wbq *WriteBatchQueue) ExecTasks() error {
	return nil
}
