package main

import (
	"fmt"
	"github.com/kevwan/mapreduce"
	"github.com/pkg/errors"
	"net"
	"time"
)

type TargetMachine struct {
	MachineID int32
	Addr string
}

type ResItem struct {
	MachineID            int32
	IsConnect            bool
	Message              string
}

func main() {
	fmt.Println(time.Now())
	targetMachines := []TargetMachine{
		{
			MachineID: 1,
			Addr: "172.16.1.121:30918",
		},
		{
			MachineID: 2,
			Addr: "172.16.1.121:32332",
		},
	}
	val, err := mapreduce.MapReduce(func(source chan<- interface{}) {
		// generator
		for _, v := range targetMachines {
			source <- v
		}
	}, func(item interface{}, writer mapreduce.Writer, cancel func(error)) {
		// mapper
		resItem := ResItem{}
		i := item.(TargetMachine)
		err := Telnet(i.Addr)
		resItem.MachineID = i.MachineID
		if err != nil {
			resItem.IsConnect = false
			resItem.Message = err.Error()
		} else {
			resItem.IsConnect = true
			resItem.Message = fmt.Sprintf("dial [%s] success", i.Addr)
		}
		writer.Write(resItem)
	}, func(pipe <-chan interface{}, writer mapreduce.Writer, cancel func(error)) {
		// reducer
		var res []ResItem
		for i := range pipe {
			res = append(res, i.(ResItem))
		}
		writer.Write(res)
	})
	fmt.Println(time.Now())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(val)
}

func Telnet(address string) error {
	time.Sleep(time.Second*5)
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		return errors.Wrapf(err, "dail [%s] in 1s", address)
	}
	if conn == nil {
		return errors.Wrapf(err, "cannot build connection when dail [%s] in 3s", address)
	}
	defer conn.Close()
	return nil
}