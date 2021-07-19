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
	for i:=0; i< batch.BatchListIndex;i++ {
		s, _ := batch.BatchList[i].Serialize()
		lst = append(lst, s)
	}
	fmt.Printf("commit lst is %v\n",lst)
	time.Sleep(100 * time.Millisecond)
}

func TestWriteBatchQueue_Baseop(t *testing.T) {
	q:=NewWriteBatchQueue(3,10,&MockStorage{})
	//push
	for i:=0;i<10;i++ {
		mockItem:=&MockItem{i}
		q.pushback(mockItem)
		q.traversal()
	}

	//pop
	for b := q.popBatch(); b != nil; b = q.popBatch() {
		fmt.Println("----------------")
		fmt.Println(b)
		q.traversal()
		fmt.Println("----------------")
	}
}


//妈的，死锁了估计。。。。
func TestWriteBatchQueue_Enqueue(t *testing.T) {
	q:=NewWriteBatchQueue(10,100000,&MockStorage{})
	go func() {
		for {
			q.traversal()
			time.Sleep(1* time.Second)
		}
	}()


	go func() {
		for i:=0;i<1000000;i++{
			mockItem:=&MockItem{i}
			q.Enqueue(mockItem)
			//q.traversal(1)
			time.Sleep(10* time.Millisecond)
		}
	}()

	go func() {
		for i:=1000000;i<2000000;i++{
			mockItem:=&MockItem{i}
			err:= q.Enqueue(mockItem)
			//q.traversal(2)
			if err != nil {
				fmt.Println("222222222",err)
			}
			time.Sleep(10* time.Millisecond)
		}
	}()

	time.Sleep(1000 * time.Second)
}

