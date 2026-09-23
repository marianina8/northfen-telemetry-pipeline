package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/marianina8/northfen-telemetry-pipeline/internal/sim"
	"github.com/marianina8/northfen-telemetry-pipeline/internal/telemetry"
)

// DynamoAPI is the subset of the DynamoDB client the store uses (fakeable).
type DynamoAPI interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
	BatchWriteItem(ctx context.Context, in *dynamodb.BatchWriteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// GSI1 is the app table's secondary index (see infra/template.yaml).
const GSI1 = "gsi1"

// Dynamo is the AWS store: three tables from infra/template.yaml.
//
//	ReadingsTable  the sensor time series (the Timestream fallback):
//	               pk series = "<session>#<equipment_id>#<sensor_id>",
//	               sk ts     = fixed-width UTC timestamp (sortable).
//	               Range reads for the explain step are one Query.
//	StateTable     detector state, externalized because the Lambda consumer
//	               is stateless: pk session_id, sk series = "<equipment_id>#<sensor_id>".
//	AppTable       windows, alerts (with audit trail), runs, session counters
//	               and equipment history, single-table style (pk/sk + gsi1).
//
// Every record is stored whole as a JSON "doc" attribute; key attributes
// are copied alongside. Sandbox records carry expires_at (DynamoDB TTL);
// reads also hide expired records immediately, since TTL deletes lazily.
type Dynamo struct {
	Client        DynamoAPI
	ReadingsTable string
	StateTable    string
	AppTable      string
	Now           func() time.Time
}

func (d *Dynamo) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d *Dynamo) expired(exp int64) bool { return exp > 0 && d.now().Unix() > exp }

func s(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func n(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func getS(item map[string]types.AttributeValue, k string) string {
	if v, ok := item[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func getN(item map[string]types.AttributeValue, k string) int64 {
	if v, ok := item[k].(*types.AttributeValueMemberN); ok {
		x, _ := strconv.ParseInt(v.Value, 10, 64)
		return x
	}
	return 0
}

func docItem(v any, keys map[string]types.AttributeValue, expiresAt int64) (map[string]types.AttributeValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	item := map[string]types.AttributeValue{"doc": s(string(b))}
	for k, v := range keys {
		item[k] = v
	}
	if expiresAt > 0 {
		item["expires_at"] = n(expiresAt)
	}
	return item, nil
}

func decode[T any](item map[string]types.AttributeValue) (T, error) {
	var out T
	err := json.Unmarshal([]byte(getS(item, "doc")), &out)
	return out, err
}

// batchPut writes items 25 at a time, retrying unprocessed items.
func (d *Dynamo) batchPut(ctx context.Context, table string, items []map[string]types.AttributeValue) error {
	for i := 0; i < len(items); i += 25 {
		reqs := make([]types.WriteRequest, 0, 25)
		for _, it := range items[i:min(i+25, len(items))] {
			reqs = append(reqs, types.WriteRequest{PutRequest: &types.PutRequest{Item: it}})
		}
		pending := map[string][]types.WriteRequest{table: reqs}
		for attempt := 0; len(pending[table]) > 0; attempt++ {
			if attempt > 8 {
				return fmt.Errorf("dynamodb batch write to %s: unprocessed items after retries", table)
			}
			if attempt > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Duration(25<<attempt) * time.Millisecond):
				}
			}
			out, err := d.Client.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: pending})
			if err != nil {
				return fmt.Errorf("dynamodb batch write to %s: %w", table, err)
			}
			pending = out.UnprocessedItems
		}
	}
	return nil
}

// query runs a Query to completion (following pagination).
func (d *Dynamo) query(ctx context.Context, in *dynamodb.QueryInput) ([]map[string]types.AttributeValue, error) {
	var out []map[string]types.AttributeValue
	for {
		res, err := d.Client.Query(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("dynamodb query %s: %w", aws.ToString(in.TableName), err)
		}
		out = append(out, res.Items...)
		if len(res.LastEvaluatedKey) == 0 || (in.Limit != nil && len(out) >= int(*in.Limit)) {
			return out, nil
		}
		in.ExclusiveStartKey = res.LastEvaluatedKey
	}
}

func pkQuery(table, index, pkName, pk string) *dynamodb.QueryInput {
	q := &dynamodb.QueryInput{
		TableName:                 aws.String(table),
		KeyConditionExpression:    aws.String("#pk = :pk"),
		ExpressionAttributeNames:  map[string]string{"#pk": pkName},
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": s(pk)},
	}
	if index != "" {
		q.IndexName = aws.String(index)
	}
	return q
}

func (d *Dynamo) get(ctx context.Context, table string, key map[string]types.AttributeValue) (map[string]types.AttributeValue, error) {
	out, err := d.Client.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(table), Key: key, ConsistentRead: aws.Bool(true)})
	if err != nil {
		return nil, fmt.Errorf("dynamodb get %s: %w", table, err)
	}
	if len(out.Item) == 0 || d.expired(getN(out.Item, "expires_at")) {
		return nil, ErrNotFound
	}
	return out.Item, nil
}

func isCondFailed(err error) bool {
	var cf *types.ConditionalCheckFailedException
	return errors.As(err, &cf)
}

// ---- time series ------------------------------------------------------------

// PutReadings implements Store.
func (d *Dynamo) PutReadings(ctx context.Context, rs []telemetry.Reading, expiresAt int64) error {
	items := make([]map[string]types.AttributeValue, 0, len(rs))
	seen := map[string]bool{} // a batch may not write the same key twice
	for _, r := range rs {
		series, ts := ReadingSeries(r.SessionID, r.EquipmentID, r.SensorID), TSKey(r.TS)
		if seen[series+"|"+ts] {
			continue
		}
		seen[series+"|"+ts] = true
		it, err := docItem(r, map[string]types.AttributeValue{"series": s(series), "ts": s(ts)}, expiresAt)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	return d.batchPut(ctx, d.ReadingsTable, items)
}

// Readings implements Store (inclusive range, oldest first).
func (d *Dynamo) Readings(ctx context.Context, sessionID, equipmentID, sensorID string, from, to time.Time) ([]telemetry.Reading, error) {
	q := &dynamodb.QueryInput{
		TableName:                aws.String(d.ReadingsTable),
		KeyConditionExpression:   aws.String("#pk = :pk AND #sk BETWEEN :lo AND :hi"),
		ExpressionAttributeNames: map[string]string{"#pk": "series", "#sk": "ts"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": s(ReadingSeries(sessionID, equipmentID, sensorID)), ":lo": s(TSKey(from)), ":hi": s(TSKey(to)),
		},
	}
	items, err := d.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]telemetry.Reading, 0, len(items))
	for _, it := range items {
		r, err := decode[telemetry.Reading](it)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// ---- detector state -----------------------------------------------------------

// GetStates implements Store.
func (d *Dynamo) GetStates(ctx context.Context, sessionID string, keys []string) (map[string]SeriesState, error) {
	all, err := d.ListStates(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	want := map[string]bool{}
	for _, k := range keys {
		want[k] = true
	}
	out := map[string]SeriesState{}
	for _, st := range all {
		if want[st.Key()] {
			out[st.Key()] = st
		}
	}
	return out, nil
}

// PutStates implements Store.
func (d *Dynamo) PutStates(ctx context.Context, states []SeriesState) error {
	items := make([]map[string]types.AttributeValue, 0, len(states))
	for _, st := range states {
		it, err := docItem(st, map[string]types.AttributeValue{"session_id": s(st.SessionID), "series": s(st.Key())}, st.ExpiresAt)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	return d.batchPut(ctx, d.StateTable, items)
}

// ListStates implements Store.
func (d *Dynamo) ListStates(ctx context.Context, sessionID string) ([]SeriesState, error) {
	q := pkQuery(d.StateTable, "", "session_id", sessionID)
	q.ConsistentRead = aws.Bool(true) // the consumer's checkpoint must be current
	items, err := d.query(ctx, q)
	if err != nil {
		return nil, err
	}
	var out []SeriesState
	for _, it := range items {
		if d.expired(getN(it, "expires_at")) {
			continue
		}
		st, err := decode[SeriesState](it)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// ---- windows ------------------------------------------------------------------

func windowKeys(w Window) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": s("WINDOW#" + w.ID), "sk": s("WINDOW"),
		"gsi1pk": s("RUNWIN#" + w.SessionID + "#" + w.RunID),
		"gsi1sk": s(fmt.Sprintf("%s#%s#%06d", w.EquipmentID, w.SensorID, w.StartTick)),
	}
}

// PutWindows implements Store.
func (d *Dynamo) PutWindows(ctx context.Context, ws []Window) error {
	items := make([]map[string]types.AttributeValue, 0, len(ws))
	for _, w := range ws {
		it, err := docItem(w, windowKeys(w), w.ExpiresAt)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	return d.batchPut(ctx, d.AppTable, items)
}

// GetWindow implements Store.
func (d *Dynamo) GetWindow(ctx context.Context, id string) (Window, error) {
	it, err := d.get(ctx, d.AppTable, map[string]types.AttributeValue{"pk": s("WINDOW#" + id), "sk": s("WINDOW")})
	if err != nil {
		return Window{}, fmt.Errorf("window %s: %w", id, err)
	}
	return decode[Window](it)
}

// ListWindows implements Store.
func (d *Dynamo) ListWindows(ctx context.Context, sessionID, runID string) ([]Window, error) {
	items, err := d.query(ctx, pkQuery(d.AppTable, GSI1, "gsi1pk", "RUNWIN#"+sessionID+"#"+runID))
	if err != nil {
		return nil, err
	}
	var out []Window
	for _, it := range items {
		if d.expired(getN(it, "expires_at")) {
			continue
		}
		w, err := decode[Window](it)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	sortWindows(out)
	return out, nil
}

// ---- alerts -------------------------------------------------------------------

func alertItem(a Alert) (map[string]types.AttributeValue, error) {
	return docItem(a, map[string]types.AttributeValue{
		"pk": s("ALERT#" + a.ID), "sk": s("ALERT"), "version": n(int64(a.Version)),
		"gsi1pk": s("SALERT#" + a.SessionID), "gsi1sk": s(a.CreatedAt.UTC().Format(time.RFC3339Nano) + "#" + a.ID),
	}, a.ExpiresAt)
}

// CreateAlert implements Store.
func (d *Dynamo) CreateAlert(ctx context.Context, a Alert) (bool, error) {
	a.Version = 1
	it, err := alertItem(a)
	if err != nil {
		return false, err
	}
	_, err = d.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.AppTable), Item: it,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if isCondFailed(err) {
		return false, nil // replayed batch: already created
	}
	if err != nil {
		return false, fmt.Errorf("dynamodb create alert %s: %w", a.ID, err)
	}
	return true, nil
}

// GetAlert implements Store.
func (d *Dynamo) GetAlert(ctx context.Context, id string) (Alert, error) {
	it, err := d.get(ctx, d.AppTable, map[string]types.AttributeValue{"pk": s("ALERT#" + id), "sk": s("ALERT")})
	if err != nil {
		return Alert{}, fmt.Errorf("alert %s: %w", id, err)
	}
	return decode[Alert](it)
}

// UpdateAlert implements Store (optimistic concurrency on version).
func (d *Dynamo) UpdateAlert(ctx context.Context, a *Alert) error {
	next := *a
	next.Version = a.Version + 1
	it, err := alertItem(next)
	if err != nil {
		return err
	}
	_, err = d.Client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(d.AppTable), Item: it,
		ConditionExpression:       aws.String("#v = :v"),
		ExpressionAttributeNames:  map[string]string{"#v": "version"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":v": n(int64(a.Version))},
	})
	if isCondFailed(err) {
		return fmt.Errorf("alert %s: %w", a.ID, ErrConflict)
	}
	if err != nil {
		return fmt.Errorf("dynamodb update alert %s: %w", a.ID, err)
	}
	a.Version = next.Version
	return nil
}

// ListAlerts implements Store (newest first). An empty session scans every
// session (operator tooling only; the public UI always passes a session).
func (d *Dynamo) ListAlerts(ctx context.Context, sessionID string) ([]Alert, error) {
	var items []map[string]types.AttributeValue
	var err error
	if sessionID != "" {
		items, err = d.query(ctx, pkQuery(d.AppTable, GSI1, "gsi1pk", "SALERT#"+sessionID))
	} else {
		items, err = d.scanPrefix(ctx, "ALERT#")
	}
	if err != nil {
		return nil, err
	}
	var out []Alert
	for _, it := range items {
		if d.expired(getN(it, "expires_at")) {
			continue
		}
		a, err := decode[Alert](it)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	SortAlerts(out)
	return out, nil
}

func (d *Dynamo) scanPrefix(ctx context.Context, prefix string) ([]map[string]types.AttributeValue, error) {
	in := &dynamodb.ScanInput{TableName: aws.String(d.AppTable)}
	var out []map[string]types.AttributeValue
	for {
		res, err := d.Client.Scan(ctx, in)
		if err != nil {
			return nil, fmt.Errorf("dynamodb scan: %w", err)
		}
		for _, it := range res.Items {
			if strings.HasPrefix(getS(it, "pk"), prefix) {
				out = append(out, it)
			}
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		in.ExclusiveStartKey = res.LastEvaluatedKey
	}
}

// ---- runs and session counters -----------------------------------------------

// PutRun implements Store.
func (d *Dynamo) PutRun(ctx context.Context, r Run) error {
	it, err := docItem(r, map[string]types.AttributeValue{"pk": s("SESSION#" + r.SessionID), "sk": s("RUN#" + r.ID)}, r.ExpiresAt)
	if err != nil {
		return err
	}
	_, err = d.Client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(d.AppTable), Item: it})
	if err != nil {
		return fmt.Errorf("dynamodb put run: %w", err)
	}
	return nil
}

// GetRun implements Store.
func (d *Dynamo) GetRun(ctx context.Context, sessionID, runID string) (Run, error) {
	it, err := d.get(ctx, d.AppTable, map[string]types.AttributeValue{"pk": s("SESSION#" + sessionID), "sk": s("RUN#" + runID)})
	if err != nil {
		return Run{}, fmt.Errorf("run %s: %w", runID, err)
	}
	return decode[Run](it)
}

// ListRuns implements Store (newest first).
func (d *Dynamo) ListRuns(ctx context.Context, sessionID string) ([]Run, error) {
	q := &dynamodb.QueryInput{
		TableName:                aws.String(d.AppTable),
		KeyConditionExpression:   aws.String("#pk = :pk AND begins_with(#sk, :sk)"),
		ExpressionAttributeNames: map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": s("SESSION#" + sessionID), ":sk": s("RUN#"),
		},
	}
	items, err := d.query(ctx, q)
	if err != nil {
		return nil, err
	}
	var out []Run
	for _, it := range items {
		if d.expired(getN(it, "expires_at")) {
			continue
		}
		r, err := decode[Run](it)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out, nil
}

// GetSession implements Store.
func (d *Dynamo) GetSession(ctx context.Context, id string) (Session, error) {
	it, err := d.get(ctx, d.AppTable, map[string]types.AttributeValue{"pk": s("SESSION#" + id), "sk": s("META")})
	if err != nil {
		return Session{ID: id}, fmt.Errorf("session %s: %w", id, err)
	}
	return Session{ID: id, Runs: int(getN(it, "runs")), Explains: int(getN(it, "explains")), ExpiresAt: getN(it, "expires_at")}, nil
}

// Incr implements Store: one atomic, conditional UpdateItem.
func (d *Dynamo) Incr(ctx context.Context, id, counter string, max int, expiresAt int64) (int, error) {
	if counter != "runs" && counter != "explains" {
		return 0, fmt.Errorf("unknown counter %q", counter)
	}
	in := &dynamodb.UpdateItemInput{
		TableName:                aws.String(d.AppTable),
		Key:                      map[string]types.AttributeValue{"pk": s("SESSION#" + id), "sk": s("META")},
		UpdateExpression:         aws.String("ADD #c :one"),
		ExpressionAttributeNames: map[string]string{"#c": counter},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": n(1),
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	}
	if expiresAt > 0 {
		in.UpdateExpression = aws.String("ADD #c :one SET #e = :e")
		in.ExpressionAttributeNames["#e"] = "expires_at"
		in.ExpressionAttributeValues[":e"] = n(expiresAt)
	}
	if max > 0 {
		in.ConditionExpression = aws.String("attribute_not_exists(#c) OR #c < :max")
		in.ExpressionAttributeValues[":max"] = n(int64(max))
	}
	out, err := d.Client.UpdateItem(ctx, in)
	if isCondFailed(err) {
		return max, ErrLimit
	}
	if err != nil {
		return 0, fmt.Errorf("dynamodb incr %s: %w", counter, err)
	}
	return int(getN(out.Attributes, counter)), nil
}

// ---- equipment history ------------------------------------------------------------

// PutHistory implements Store. Keys are the record's position per tool, so
// re-seeding (dates are "days ago", relative to seeding time) overwrites
// instead of duplicating.
func (d *Dynamo) PutHistory(ctx context.Context, hs []sim.HistoryRecord) error {
	items := make([]map[string]types.AttributeValue, 0, len(hs))
	pos := map[string]int{}
	for _, h := range hs {
		i := pos[h.EquipmentID]
		pos[h.EquipmentID]++
		it, err := docItem(h, map[string]types.AttributeValue{
			"pk": s("HISTORY#" + h.EquipmentID), "sk": s(fmt.Sprintf("%03d", i)),
		}, 0)
		if err != nil {
			return err
		}
		items = append(items, it)
	}
	return d.batchPut(ctx, d.AppTable, items)
}

// History implements Store (newest first).
func (d *Dynamo) History(ctx context.Context, equipmentID string, limit int) ([]sim.HistoryRecord, error) {
	items, err := d.query(ctx, pkQuery(d.AppTable, "", "pk", "HISTORY#"+equipmentID))
	if err != nil {
		return nil, err
	}
	var out []sim.HistoryRecord
	for _, it := range items {
		h, err := decode[sim.HistoryRecord](it)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Date.After(out[j].Date) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

var _ Store = (*Dynamo)(nil)
