package gohbase

import (
	"context"
	"fmt"
	"github.com/kuhufu/gohbase/thrift/thrift2/hbase"
	"strconv"
	"testing"
)

const (
	Table  = "friend_circle_message"
	Family = "user_family"
)

var testConn *conn

func init() {
	var err error
	testConn, err = newConn("192.168.200.203:9090")
	if err != nil {
		panic(err)
	}
}

func TestHBaseService_Put(t *testing.T) {
	for i := 0; i < 20; i++ {
		idx := fmt.Sprintf("%03d", i)
		err := testConn.Put(context.Background(), []byte(Table), &hbase.TPut{
			Row: []byte("r" + idx),
			ColumnValues: []*hbase.TColumnValue{
				{
					Family:    []byte(Family),
					Qualifier: []byte("name"),
					Value:     []byte("kuhufu" + idx),
				},
				{
					Family:    []byte(Family),
					Qualifier: []byte("age"),
					Value:     []byte(idx),
				},
			},
		})

		if err != nil {
			t.Error(err)
		}
	}
}

func TestHBaseService_Get(t *testing.T) {
	res, err := testConn.Get(context.Background(), []byte(Table), &hbase.TGet{
		Row: []byte("r002"),
	})

	if err != nil {
		t.Error(err)
	}

	for _, col := range res.ColumnValues {
		fmt.Println(string(col.Family))
		fmt.Println(string(col.Qualifier))
		fmt.Println(string(col.Value))
	}
}

func TestHBaseService_DeleteSingle(t *testing.T) {
	for i := 0; i < 20; i++ {
		idx := strconv.Itoa(i)
		err := testConn.DeleteSingle(context.Background(), []byte(Table), &hbase.TDelete{
			Row: []byte("r" + idx),
		})

		if err != nil {
			t.Error(err)
		}
	}
}

func TestHBaseService_Exists(t *testing.T) {
	res, err := testConn.Exists(context.Background(), []byte(Table), &hbase.TGet{
		Row: []byte("r1"),
		Columns: []*hbase.TColumn{
			{
				Family:    []byte(Family),
				Qualifier: []byte("name"),
			},
		},
	})

	if err != nil {
		t.Error(err)
	}

	t.Log(res)
}

func TestHBaseService_GetScannerResults(t *testing.T) {
	limit := int32(10)
	reversed := true

	rows, err := testConn.GetScannerResults(context.Background(),
		[]byte(Table),
		&hbase.TScan{
			StartRow: []byte("r9999"),
			//StopRow:  []byte("r000"),
			Columns: []*hbase.TColumn{
				{
					Family:    []byte(Family),
					Qualifier: []byte("name"),
				},
			},
			Limit:    &limit,
			Reversed: &reversed,
		},
		20,
	)
	if err != nil {
		t.Error(err)
	}

	for _, row := range rows {
		fmt.Println(string(row.Row))
		for _, col := range row.ColumnValues {
			fmt.Println(string(col.Family))
			fmt.Println(string(col.Qualifier))
			fmt.Println(string(col.Value))

			fmt.Println()
		}
	}
}

func TestHBaseService_GetScannerRows(t *testing.T) {
	limit := int32(10)
	reversed := true
	scanner := &hbase.TScan{
		StartRow: []byte("r9999"),
		//StopRow:  []byte("r000"),
		Columns: []*hbase.TColumn{
			{
				Family:    []byte(Family),
				Qualifier: []byte("name"),
			},
		},
		Limit:    &limit,
		Reversed: &reversed,
	}

	scannerId, err := testConn.OpenScanner(context.Background(), []byte(Table), scanner)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 2; i++ {
		rows, err := testConn.GetScannerRows(context.Background(), scannerId, 1)
		if err != nil {
			t.Error(err)
		}

		for _, row := range rows {
			fmt.Println(string(row.Row))
			for _, col := range row.ColumnValues {
				fmt.Println(string(col.Family))
				fmt.Println(string(col.Qualifier))
				fmt.Println(string(col.Value))

				fmt.Println()
			}
		}
	}
}

func Test_print(t *testing.T) {
	fmt.Printf("%04d\n", 1)
}

func Test_compare(t *testing.T) {
	fmt.Println("r00" > "r0000")
	fmt.Println("1" > "01")
	fmt.Println("2" > "12")

	a := []int{1, 2}

	b := append(a[:0:0], a...)
	b[0] = 2
	fmt.Println(a)

	fmt.Println(len(a[0:0]), cap(a[0:0]))
	fmt.Println(len(a[0:0:0]), cap(a[0:0:0]))
	fmt.Println(len(a[:0:0]), cap(a[:0:0]))
}
