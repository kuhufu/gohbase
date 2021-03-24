package gohbase

import (
	"context"
	"fmt"
	"github.com/kuhufu/gohbase/thrift/thrift2/hbase"
	"strconv"
	"testing"
)

var pool *Pool

func init() {
	var err error
	pool, err = NewPool(Config{
		Addr:    "192.168.200.203:9090",
		MinConn: 100,
		MaxConn: 100,
	})

	if err != nil {
		panic(err)
	}
}

func TestConn_Put(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	for i := 0; i < 20; i++ {

		idx := fmt.Sprintf("%03d", i)
		err = conn.Put(context.Background(), []byte(Table), &hbase.TPut{
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
			switch v := err.(type) {
			case *hbase.TIOError:
				t.Log(*v.Message)
			}
		}
	}
}

func TestConn_Get(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	res, err := conn.Get(context.Background(), []byte(Table), &hbase.TGet{
		Row: []byte("r2"),
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

func TestConn_DeleteSingle(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	for i := 0; i < 20; i++ {
		idx := strconv.Itoa(i)
		err := conn.DeleteSingle(context.Background(), []byte(Table), &hbase.TDelete{
			Row: []byte("r" + idx),
		})

		if err != nil {
			t.Error(err)
		}
	}
}

func TestConn_Exists(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	tget := &hbase.TGet{
		Row: []byte("r1"),
		Columns: []*hbase.TColumn{
			{
				Family:    []byte(Family),
				Qualifier: []byte("name"),
			},
		},
	}

	res, err := conn.Exists(context.Background(), []byte(Table), tget)
	if err != nil {
		t.Error(err)
	}

	t.Log(res)


}

func TestConn_GetScannerResults(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	limit := int32(10)
	reversed := true

	rows, err := conn.GetScannerResults(context.Background(),
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

func TestConn_GetScannerRows(t *testing.T) {
	conn, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

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

	scannerId, err := conn.OpenScanner(context.Background(), []byte(Table), scanner)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 2; i++ {
		rows, err := conn.GetScannerRows(context.Background(), scannerId, 1)
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
