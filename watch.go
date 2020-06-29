package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"crawshaw.io/sqlite"
	"crawshaw.io/sqlite/sqlitex"
)

type rev int64

func (r rev) MarshalText() (text []byte, err error) {
	return []byte(fmt.Sprintf("%d", int64(r))), nil
}
func (r *rev) UnmarshalText(text []byte) error {
	_, err := fmt.Sscanf(string(text), "%d", (*int64)(r))
	return err
}

func watch(ctx context.Context, conn *sqlite.Conn, addr, authHeader, keyPrefix string, startRev rev) error {
	httpc := http.DefaultClient
	var watchRequest struct {
		CreateRequest struct {
			Key           []byte `json:"key"`
			RangeEnd      []byte `json:"range_end"`
			StartRevision rev    `json:"start_revision"`
		} `json:"create_request"`
	}
	watchRequest.CreateRequest.Key = []byte(keyPrefix)
	watchRequest.CreateRequest.RangeEnd = addOne([]byte(keyPrefix))
	watchRequest.CreateRequest.StartRevision = startRev

	data, err := json.Marshal(watchRequest)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", addr+"/v3/watch", bytes.NewReader(data))
	if err != nil {
		return err
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	req = req.WithContext(ctx)
	res, err := httpc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ := ioutil.ReadAll(res.Body)
		return fmt.Errorf("status=%d: %q", res.StatusCode, string(b))
	}

	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		if err := watchResult(conn, scanner.Bytes()); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func watchResult(conn *sqlite.Conn, res []byte) (err error) {
	defer sqlitex.Save(conn)(&err)

	type KeyValue struct {
		Key         []byte `json:"key"`
		ModRevision rev    `json:"mod_revision"`
		Value       []byte `json:"value"`
	}
	type Event struct {
		Type   string   `json:"type"` // "PUT" or "DELETE"
		KV     KeyValue `json:"kv"`
		PrevKV KeyValue `json:"prev_kv"`
	}
	var watchResult struct {
		Result struct {
			Header struct {
				Revision rev `json:"revision"`
			} `json:"header"`
			Events []Event `json:"events"`
		} `json:"result"`
	}
	if err := json.Unmarshal(res, &watchResult); err != nil {
		return err
	}

	for _, ev := range watchResult.Result.Events {
		if ev.Type == "DELETE" {
			fmt.Fprintf(os.Stderr, "etcd.watch: TODO delete key %s", ev.KV.Value)
			continue
		}

		stmt := conn.Prep(`insert or ignore into history (key, value, mod_revision) values ($key,$val,$rev);`)
		stmt.SetBytes("$key", ev.KV.Key)
		stmt.SetBytes("$val", ev.KV.Value)
		stmt.SetInt64("$rev", int64(ev.KV.ModRevision))
		if _, err := stmt.Step(); err != nil {
			return err
		}

		if ev.PrevKV.ModRevision != 0 {
			stmt.Reset()
			stmt.SetBytes("$key", ev.PrevKV.Key)
			stmt.SetBytes("$val", ev.PrevKV.Value)
			stmt.SetInt64("$rev", int64(ev.PrevKV.ModRevision))
			if _, err := stmt.Step(); err != nil {
				return err
			}
		}

		stmt = conn.Prep(`insert or ignore into revtime (mod_revision, watch_time) values ($rev,$time);`)
		stmt.SetInt64("$rev", int64(ev.KV.ModRevision))
		stmt.SetInt64("$time", time.Now().UTC().UnixNano())
		if _, err := stmt.Step(); err != nil {
			return err
		}
	}
	return nil
}

// addOne modifies v to be the next key in lexicographic order.
func addOne(v []byte) []byte {
	for len(v) > 0 && v[len(v)-1] == 0xff {
		v = v[:len(v)-1]
	}
	if len(v) > 0 {
		v[len(v)-1]++
	}
	return v
}

func fillTo(ctx context.Context, conn *sqlite.Conn, maxModRev rev) error {
	type interval struct{ a, b int64 } // open interval (a,b)
	var intervals []interval

	stmt := conn.Prep(`select distinct mod_revision from history order by mod_revision desc;`)
	lastRev := int64(-1)
	for {
		if hasNext, err := stmt.Step(); err != nil {
			return err
		} else if !hasNext {
			break
		}
		modRev := stmt.GetInt64("mod_revision")
		if lastRev != -1 {
			diff := lastRev - modRev
			if diff > 1 {
				intervals = append(intervals, interval{a: modRev, b: lastRev})
			}
		}
		lastRev = modRev
	}
	if len(intervals) == 0 {
		return nil
	}
	return fmt.Errorf("TODO backfill intervals: %v", intervals)
}
