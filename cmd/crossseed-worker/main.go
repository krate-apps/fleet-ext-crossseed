// Command crossseed-worker pushes completed rTorrent downloads to other clients.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type settings struct {
	Enabled           bool     `json:"enabled"`
	IntervalSec       int      `json:"intervalSec"`
	OnlyComplete      bool     `json:"onlyComplete"`
	SkipIfPresent     bool     `json:"skipIfPresent"`
	TargetGatewayIDs  []string `json:"targetGatewayIds"`
}

type torrentRow struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	Complete  bool   `json:"complete"`
	Directory string `json:"directory"`
}

type sentKey struct {
	Hash      string `json:"hash"`
	GatewayID string `json:"gatewayId"`
}

type eventRow struct {
	ID        string `json:"id"`
	Time      int64  `json:"time"`
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	GatewayID string `json:"gatewayId"`
	OK        bool   `json:"ok"`
	Skipped   bool   `json:"skipped,omitempty"`
	Message   string `json:"message,omitempty"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopScan()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := append([]byte(nil), in.Bytes()...)
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		// Handle requests concurrently so hostCall reverse-RPC can complete
		// while this stdin loop keeps reading responses.
		go handleRequest(req)
	}
}

func handleRequest(req request) {
	switch req.Method {
	case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
		reply(req.ID, map[string]any{"ok": true})
	case "fleet.shutdown":
		reply(req.ID, map[string]any{"ok": true})
		os.Exit(0)
	case "ext.crossseed.getSettings":
		st, err := loadSettings()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{
			"enabled":              st.Enabled,
			"intervalSec":          st.IntervalSec,
			"onlyComplete":         st.OnlyComplete,
			"skipIfPresent":        st.SkipIfPresent,
			"targetGatewayIds":     st.TargetGatewayIDs,
			"targetGatewayIdsText": strings.Join(st.TargetGatewayIDs, "\n"),
		})
	case "ext.crossseed.saveSettings":
		var p struct {
			Enabled              bool     `json:"enabled"`
			IntervalSec          int      `json:"intervalSec"`
			OnlyComplete         bool     `json:"onlyComplete"`
			SkipIfPresent        bool     `json:"skipIfPresent"`
			TargetGatewayIdsText string   `json:"targetGatewayIdsText"`
			TargetGatewayIds     []string `json:"targetGatewayIds"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			replyErr(req.ID, "invalid settings")
			return
		}
		ids := p.TargetGatewayIds
		if len(ids) == 0 {
			ids = splitLines(p.TargetGatewayIdsText)
		}
		st := settings{
			Enabled:          p.Enabled,
			IntervalSec:      p.IntervalSec,
			OnlyComplete:     p.OnlyComplete,
			SkipIfPresent:    p.SkipIfPresent,
			TargetGatewayIDs: ids,
		}
		if err := validateSettings(st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		if err := saveSettings(&st); err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{
			"enabled":              st.Enabled,
			"intervalSec":          st.IntervalSec,
			"onlyComplete":         st.OnlyComplete,
			"skipIfPresent":        st.SkipIfPresent,
			"targetGatewayIds":     st.TargetGatewayIDs,
			"targetGatewayIdsText": strings.Join(st.TargetGatewayIDs, "\n"),
		})
	case "ext.crossseed.scan":
		res, err := scanOnce()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.crossseed.crossSeed":
		var p struct {
			Hashes []string `json:"hashes"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil {
			replyErr(req.ID, "invalid params")
			return
		}
		res, err := crossSeedHashes(p.Hashes, true)
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, res)
	case "ext.crossseed.listEvents":
		items, err := loadEvents()
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, map[string]any{"items": items})
	case "ext.crossseed.listGateways":
		raw, err := hostCall("fleet.gateways.list", map[string]any{})
		if err != nil {
			replyErr(req.ID, err.Error())
			return
		}
		reply(req.ID, json.RawMessage(raw))
	default:
		replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
	}
}

func loopScan() {
	time.Sleep(3 * time.Second)
	for {
		st, err := loadSettings()
		interval := 30
		if err == nil && st.IntervalSec > 0 {
			interval = st.IntervalSec
		}
		if err == nil && st.Enabled {
			_, _ = scanOnce()
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func defaultSettings() settings {
	return settings{
		Enabled:       false,
		IntervalSec:   30,
		OnlyComplete:  true,
		SkipIfPresent: true,
	}
}

func validateSettings(st settings) error {
	if st.IntervalSec < 5 || st.IntervalSec > 3600 {
		return fmt.Errorf("intervalSec must be between 5 and 3600")
	}
	return nil
}

func loadSettings() (settings, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "settings"})
	if err != nil {
		return defaultSettings(), nil
	}
	if string(raw) == "null" || len(raw) == 0 {
		return defaultSettings(), nil
	}
	var wrap struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err == nil && len(wrap.Value) > 0 {
		raw = wrap.Value
	}
	st := defaultSettings()
	if err := json.Unmarshal(raw, &st); err != nil {
		return defaultSettings(), nil
	}
	if st.IntervalSec == 0 {
		st.IntervalSec = 30
	}
	return st, nil
}

func saveSettings(st *settings) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "settings", "value": st})
	return err
}

func loadSnapshot() (map[string]bool, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "snapshot"})
	if err != nil || string(raw) == "null" || len(raw) == 0 {
		return map[string]bool{}, nil
	}
	var wrap struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err == nil && len(wrap.Value) > 0 {
		raw = wrap.Value
	}
	out := map[string]bool{}
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func saveSnapshot(m map[string]bool) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "snapshot", "value": m})
	return err
}

func loadSent() (map[string]bool, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "sent"})
	if err != nil || string(raw) == "null" || len(raw) == 0 {
		return map[string]bool{}, nil
	}
	var wrap struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err == nil && len(wrap.Value) > 0 {
		raw = wrap.Value
	}
	out := map[string]bool{}
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func saveSent(m map[string]bool) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "sent", "value": m})
	return err
}

func sentMapKey(hash, gatewayID string) string {
	return strings.ToUpper(hash) + "|" + gatewayID
}

func loadEvents() ([]eventRow, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "events"})
	if err != nil || string(raw) == "null" || len(raw) == 0 {
		return []eventRow{}, nil
	}
	var wrap struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(raw, &wrap); err == nil && len(wrap.Value) > 0 {
		raw = wrap.Value
	}
	var items []eventRow
	_ = json.Unmarshal(raw, &items)
	return items, nil
}

func appendEvent(ev eventRow) error {
	items, err := loadEvents()
	if err != nil {
		return err
	}
	items = append([]eventRow{ev}, items...)
	if len(items) > 200 {
		items = items[:200]
	}
	_, err = hostCall("fleet.storage.put", map[string]any{"key": "events", "value": items})
	return err
}

func scanOnce() (map[string]any, error) {
	st, err := loadSettings()
	if err != nil {
		return nil, err
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Torrents []torrentRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}
	prev, err := loadSnapshot()
	if err != nil {
		return nil, err
	}
	first := len(prev) == 0
	cur := map[string]bool{}
	var newlyComplete []string
	for _, t := range wrap.Torrents {
		hash := strings.ToUpper(strings.TrimSpace(t.Hash))
		if hash == "" {
			continue
		}
		cur[hash] = t.Complete
		if first {
			continue
		}
		wasComplete := prev[hash]
		if t.Complete && !wasComplete {
			newlyComplete = append(newlyComplete, hash)
		}
	}
	if err := saveSnapshot(cur); err != nil {
		return nil, err
	}
	if first || !st.Enabled || len(newlyComplete) == 0 {
		return map[string]any{
			"ok":            true,
			"first":         first,
			"newlyComplete": len(newlyComplete),
			"sent":          0,
		}, nil
	}
	res, err := crossSeedHashes(newlyComplete, false)
	if err != nil {
		return nil, err
	}
	res["newlyComplete"] = len(newlyComplete)
	res["first"] = false
	return res, nil
}

func crossSeedHashes(hashes []string, manual bool) (map[string]any, error) {
	st, err := loadSettings()
	if err != nil {
		return nil, err
	}
	if len(st.TargetGatewayIDs) == 0 {
		return map[string]any{"ok": true, "sent": 0, "failed": 0, "message": "no targets configured"}, nil
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var wrap struct {
		Torrents []torrentRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, err
	}
	byHash := map[string]torrentRow{}
	for _, t := range wrap.Torrents {
		byHash[strings.ToUpper(strings.TrimSpace(t.Hash))] = t
	}
	sent, err := loadSent()
	if err != nil {
		return nil, err
	}
	sentCount, failCount, skipCount := 0, 0, 0
	for _, h := range hashes {
		hash := strings.ToUpper(strings.TrimSpace(h))
		t, ok := byHash[hash]
		if !ok {
			failCount++
			continue
		}
		if st.OnlyComplete && !t.Complete && !manual {
			continue
		}
		if strings.TrimSpace(t.Directory) == "" {
			failCount++
			_ = appendEvent(eventRow{
				ID: fmt.Sprintf("%d-%s", time.Now().UnixNano(), hash[:8]),
				Time: time.Now().Unix(), Hash: hash, Name: t.Name,
				OK: false, Message: "missing directory",
			})
			continue
		}
		srcRaw, err := hostCall("fleet.torrents.source", map[string]any{"hash": hash})
		if err != nil {
			failCount++
			_ = appendEvent(eventRow{
				ID: fmt.Sprintf("%d-%s", time.Now().UnixNano(), hash[:8]),
				Time: time.Now().Unix(), Hash: hash, Name: t.Name,
				OK: false, Message: err.Error(),
			})
			continue
		}
		var src struct {
			Name          string `json:"name"`
			TorrentBase64 string `json:"torrentBase64"`
		}
		if err := json.Unmarshal(srcRaw, &src); err != nil || src.TorrentBase64 == "" {
			failCount++
			continue
		}
		for _, gwID := range st.TargetGatewayIDs {
			gwID = strings.TrimSpace(gwID)
			if gwID == "" {
				continue
			}
			key := sentMapKey(hash, gwID)
			if sent[key] && st.SkipIfPresent && !manual {
				skipCount++
				continue
			}
			addRaw, err := hostCall("fleet.gateways.addTorrent", map[string]any{
				"gatewayId":     gwID,
				"filename":      src.Name,
				"torrentBase64": src.TorrentBase64,
				"savePath":      t.Directory,
				"skipIfPresent": st.SkipIfPresent,
			})
			ev := eventRow{
				ID:        fmt.Sprintf("%d-%s-%s", time.Now().UnixNano(), hash[:8], gwID),
				Time:      time.Now().Unix(),
				Hash:      hash,
				Name:      t.Name,
				GatewayID: gwID,
			}
			if err != nil {
				failCount++
				ev.OK = false
				ev.Message = err.Error()
				_ = appendEvent(ev)
				continue
			}
			var addRes struct {
				OK      bool   `json:"ok"`
				Skipped bool   `json:"skipped"`
				Message string `json:"message"`
			}
			_ = json.Unmarshal(addRaw, &addRes)
			ev.OK = addRes.OK
			ev.Skipped = addRes.Skipped
			ev.Message = addRes.Message
			_ = appendEvent(ev)
			if addRes.OK {
				sent[key] = true
				if addRes.Skipped {
					skipCount++
				} else {
					sentCount++
				}
			} else {
				failCount++
			}
		}
	}
	_ = saveSent(sent)
	return map[string]any{
		"ok":      true,
		"sent":    sentCount,
		"skipped": skipCount,
		"failed":  failCount,
	}, nil
}

func splitLines(s string) []string {
	out := []string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()

	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()

	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(90 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
