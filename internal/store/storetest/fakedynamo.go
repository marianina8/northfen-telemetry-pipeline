// Package storetest has a small in-memory DynamoDB fake (just the calls and
// expression shapes store.Dynamo uses) and a contract test suite every Store
// implementation must pass.
package storetest

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type item = map[string]types.AttributeValue

type table struct {
	pk, sk  string
	indexes map[string][2]string
	items   map[string]item
}

// FakeDynamo implements store.DynamoAPI in memory.
type FakeDynamo struct {
	mu     sync.Mutex
	tables map[string]*table
	// PageSize > 0 makes Query/Scan paginate (to exercise the paging loop).
	PageSize int
	// UnprocessedOnce makes the next BatchWriteItem leave its last request
	// unprocessed (to exercise the retry loop).
	UnprocessedOnce bool
	Calls           map[string]int
}

// NewFakeDynamo creates the three tables from infra/template.yaml.
func NewFakeDynamo(readings, state, app string) *FakeDynamo {
	return &FakeDynamo{Calls: map[string]int{}, tables: map[string]*table{
		readings: {pk: "series", sk: "ts", items: map[string]item{}},
		state:    {pk: "session_id", sk: "series", items: map[string]item{}},
		app:      {pk: "pk", sk: "sk", items: map[string]item{}, indexes: map[string][2]string{"gsi1": {"gsi1pk", "gsi1sk"}}},
	}}
}

func str(it item, k string) string {
	if v, ok := it[k].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

func num(it item, k string) (float64, bool) {
	if v, ok := it[k].(*types.AttributeValueMemberN); ok {
		f, err := strconv.ParseFloat(v.Value, 64)
		return f, err == nil
	}
	return 0, false
}

func (f *FakeDynamo) tbl(name *string) (*table, error) {
	t, ok := f.tables[aws.ToString(name)]
	if !ok {
		return nil, &types.ResourceNotFoundException{Message: aws.String("no table " + aws.ToString(name))}
	}
	return t, nil
}

func (t *table) key(it item) string { return str(it, t.pk) + "\x00" + str(it, t.sk) }

func copyItem(it item) item {
	out := item{}
	for k, v := range it {
		out[k] = v
	}
	return out
}

func resolve(names map[string]string, n string) string {
	if strings.HasPrefix(n, "#") {
		return names[n]
	}
	return n
}

// GetItem implements store.DynamoAPI.
func (f *FakeDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["GetItem"]++
	t, err := f.tbl(in.TableName)
	if err != nil {
		return nil, err
	}
	it := t.items[t.key(in.Key)]
	if it == nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	return &dynamodb.GetItemOutput{Item: copyItem(it)}, nil
}

var reVersion = regexp.MustCompile(`^(#\w+) = (:\w+)$`)

// PutItem implements store.DynamoAPI.
func (f *FakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["PutItem"]++
	t, err := f.tbl(in.TableName)
	if err != nil {
		return nil, err
	}
	k := t.key(in.Item)
	cur := t.items[k]
	if c := aws.ToString(in.ConditionExpression); c != "" {
		ok := false
		switch {
		case c == "attribute_not_exists(pk)" || c == "attribute_not_exists("+t.pk+")":
			ok = cur == nil
		case reVersion.MatchString(c):
			m := reVersion.FindStringSubmatch(c)
			have, hok := num(cur, resolve(in.ExpressionAttributeNames, m[1]))
			want, _ := num(in.ExpressionAttributeValues, m[2])
			ok = cur != nil && hok && have == want
		default:
			return nil, fmt.Errorf("fake: unsupported condition %q", c)
		}
		if !ok {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("condition failed")}
		}
	}
	t.items[k] = copyItem(in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

var reUpdate = regexp.MustCompile(`^ADD (#\w+) (:\w+)(?: SET (#\w+) = (:\w+))?$`)
var reUpdCond = regexp.MustCompile(`^attribute_not_exists\((#\w+)\) OR (#\w+) < (:\w+)$`)

// UpdateItem implements store.DynamoAPI (the counter update only).
func (f *FakeDynamo) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["UpdateItem"]++
	t, err := f.tbl(in.TableName)
	if err != nil {
		return nil, err
	}
	m := reUpdate.FindStringSubmatch(aws.ToString(in.UpdateExpression))
	if m == nil {
		return nil, fmt.Errorf("fake: unsupported update %q", aws.ToString(in.UpdateExpression))
	}
	k := t.key(in.Key)
	cur := t.items[k]
	if cur == nil {
		cur = copyItem(in.Key)
	} else {
		cur = copyItem(cur)
	}
	attr := resolve(in.ExpressionAttributeNames, m[1])
	have, exists := num(cur, attr)
	if c := aws.ToString(in.ConditionExpression); c != "" {
		cm := reUpdCond.FindStringSubmatch(c)
		if cm == nil {
			return nil, fmt.Errorf("fake: unsupported condition %q", c)
		}
		limit, _ := num(in.ExpressionAttributeValues, cm[3])
		if exists && !(have < limit) {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("condition failed")}
		}
	}
	add, _ := num(in.ExpressionAttributeValues, m[2])
	cur[attr] = &types.AttributeValueMemberN{Value: strconv.FormatFloat(have+add, 'f', -1, 64)}
	if m[3] != "" {
		cur[resolve(in.ExpressionAttributeNames, m[3])] = in.ExpressionAttributeValues[m[4]]
	}
	t.items[k] = cur
	return &dynamodb.UpdateItemOutput{Attributes: item{attr: cur[attr]}}, nil
}

var (
	reQPK      = regexp.MustCompile(`^(#\w+) = (:\w+)$`)
	reQBetween = regexp.MustCompile(`^(#\w+) = (:\w+) AND (#\w+) BETWEEN (:\w+) AND (:\w+)$`)
	reQBegins  = regexp.MustCompile(`^(#\w+) = (:\w+) AND begins_with\((#\w+), (:\w+)\)$`)
)

func page(all []item, start item, size int) ([]item, item) {
	off := 0
	if start != nil {
		o, _ := num(start, "_off")
		off = int(o)
	}
	if size <= 0 || off+size >= len(all) {
		return all[min(off, len(all)):], nil
	}
	return all[off : off+size], item{"_off": &types.AttributeValueMemberN{Value: strconv.Itoa(off + size)}}
}

// Query implements store.DynamoAPI.
func (f *FakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["Query"]++
	t, err := f.tbl(in.TableName)
	if err != nil {
		return nil, err
	}
	pkName, skName := t.pk, t.sk
	if ix := aws.ToString(in.IndexName); ix != "" {
		keys, ok := t.indexes[ix]
		if !ok {
			return nil, fmt.Errorf("fake: no index %s", ix)
		}
		pkName, skName = keys[0], keys[1]
	}
	names, vals := in.ExpressionAttributeNames, in.ExpressionAttributeValues
	expr := aws.ToString(in.KeyConditionExpression)
	var match func(it item) bool
	checkNames := func(p, sk string) error {
		if resolve(names, p) != pkName || (sk != "" && resolve(names, sk) != skName) {
			return fmt.Errorf("fake: key condition %q doesn't use the key attributes %s/%s", expr, pkName, skName)
		}
		return nil
	}
	switch {
	case reQBetween.MatchString(expr):
		m := reQBetween.FindStringSubmatch(expr)
		if err := checkNames(m[1], m[3]); err != nil {
			return nil, err
		}
		pk, lo, hi := str(vals, m[2]), str(vals, m[4]), str(vals, m[5])
		match = func(it item) bool { v := str(it, skName); return str(it, pkName) == pk && v >= lo && v <= hi }
	case reQBegins.MatchString(expr):
		m := reQBegins.FindStringSubmatch(expr)
		if err := checkNames(m[1], m[3]); err != nil {
			return nil, err
		}
		pk, pre := str(vals, m[2]), str(vals, m[4])
		match = func(it item) bool { return str(it, pkName) == pk && strings.HasPrefix(str(it, skName), pre) }
	case reQPK.MatchString(expr):
		m := reQPK.FindStringSubmatch(expr)
		if err := checkNames(m[1], ""); err != nil {
			return nil, err
		}
		pk := str(vals, m[2])
		match = func(it item) bool { return str(it, pkName) == pk }
	default:
		return nil, fmt.Errorf("fake: unsupported key condition %q", expr)
	}
	var all []item
	for _, it := range t.items {
		if _, has := it[pkName]; has && match(it) {
			all = append(all, copyItem(it))
		}
	}
	sort.Slice(all, func(i, j int) bool { return str(all[i], skName) < str(all[j], skName) })
	if in.ScanIndexForward != nil && !*in.ScanIndexForward {
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	}
	items, last := page(all, in.ExclusiveStartKey, f.PageSize)
	return &dynamodb.QueryOutput{Items: items, LastEvaluatedKey: last, Count: int32(len(items))}, nil
}

// Scan implements store.DynamoAPI.
func (f *FakeDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["Scan"]++
	t, err := f.tbl(in.TableName)
	if err != nil {
		return nil, err
	}
	var all []item
	for _, it := range t.items {
		all = append(all, copyItem(it))
	}
	sort.Slice(all, func(i, j int) bool { return t.key(all[i]) < t.key(all[j]) })
	items, last := page(all, in.ExclusiveStartKey, f.PageSize)
	return &dynamodb.ScanOutput{Items: items, LastEvaluatedKey: last}, nil
}

// BatchWriteItem implements store.DynamoAPI (puts only).
func (f *FakeDynamo) BatchWriteItem(_ context.Context, in *dynamodb.BatchWriteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls["BatchWriteItem"]++
	out := &dynamodb.BatchWriteItemOutput{UnprocessedItems: map[string][]types.WriteRequest{}}
	for name, reqs := range in.RequestItems {
		t, err := f.tbl(aws.String(name))
		if err != nil {
			return nil, err
		}
		if len(reqs) > 25 {
			return nil, fmt.Errorf("fake: batch of %d > 25", len(reqs))
		}
		seen := map[string]bool{}
		for i, r := range reqs {
			if r.PutRequest == nil {
				return nil, fmt.Errorf("fake: only PutRequest is supported")
			}
			k := t.key(r.PutRequest.Item)
			if seen[k] {
				return nil, fmt.Errorf("fake: duplicate key in one batch (DynamoDB rejects this)")
			}
			seen[k] = true
			if f.UnprocessedOnce && i == len(reqs)-1 {
				f.UnprocessedOnce = false
				out.UnprocessedItems[name] = append(out.UnprocessedItems[name], r)
				continue
			}
			t.items[k] = copyItem(r.PutRequest.Item)
		}
	}
	return out, nil
}

// Count returns how many items a table holds.
func (f *FakeDynamo) Count(table string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.tables[table].items)
}
