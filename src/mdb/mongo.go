package mdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/892294101/jxutils"
	"github.com/892294101/mongosvc/src/pargs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strconv"
	"strings"
	"time"
)

type Index struct {
	Key   string
	IType interface{}
}

type MongoDB struct {
	conn           *mongo.Client
	log            *logrus.Logger
	dbNameList     map[string]map[string]string // 索引数据库名称缓存列表
	shardBlacklist map[string]string            // 分片时数据库黑名单
	shardWhitelist map[string]string            // 分片时数据库白名单
	sp             *pargs.Param
}

func (m *MongoDB) SetLogger(log *logrus.Logger) {
	m.log = log
}

func (m *MongoDB) SetLoggerParam(s *pargs.Param) {
	m.sp = s
}

func (m *MongoDB) InitMongo(url string) error {
	add := fmt.Sprintf("mongodb://%s", url)
	m.log.Infof("open Database Connection: %v", add)
	m.shardBlacklist = make(map[string]string)
	m.shardWhitelist = make(map[string]string)
	m.dbNameList = make(map[string]map[string]string)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(add))
	if err != nil {
		return err
	}
	m.conn = client
	return client.Ping(ctx, readpref.Primary())
}

func (m *MongoDB) CheckDBIsShard(d string) error {
	// 检查黑名单是否存在该数据库
	if _, ok := m.shardBlacklist[d]; ok {
		return errors.Errorf(`The database "%v" is already on the blacklist`, d)
	}

	// 检查白名单是否存在该数据库
	if _, ok := m.shardWhitelist[d]; ok {
		m.log.Infof(`The database "%v" is already on the whitelist`, d)
		return nil
	}

	var result bson.M
	opts := options.FindOne().SetProjection(bson.M{"partitioned": 1, "_id": 0})
	res := m.conn.Database("config").Collection("databases").FindOne(context.Background(), bson.D{{"_id", d}}, opts)
	if err := res.Decode(&result); err != nil {
		m.shardBlacklist[d] = d
		if err == mongo.ErrNoDocuments {
			return errors.Errorf(`database "%v" does not exist added to the blacklist`, d)
		}
		return errors.Errorf(`check database "%v" sharding status error and added to the blacklist: %v`, d, err)
	}

	// 如果分片状态是true的，直接返回
	if len(result) > 0 {
		for _, val := range result {
			m.log.Infof(`database "%v" sharding state: %v`, d, val)
			if val.(bool) {
				m.shardWhitelist[d] = d
				// 分片状态是true，则返回空
				m.log.Infof(`database "%v" has been sharded and added to the whitelist`, d)
				return nil
			} else {
				// 如果数据库分片状态是false，则为数据库开启分片. 用户指定自动开启
				if m.sp.GetEnShard() {
					m.log.Infof(`The user specified the "%v" parameter. and now enables sharding for the "%v" database`, strings.ToLower(pargs.EnableShardKey), d)
					if err := m.enableDBShared(d); err != nil {
						m.shardBlacklist[d] = d
						// 如果开启分片失败，则放到黑名单中，不在进行尝试
						return errors.Errorf(`database "%v" sharding failed add to blacklist`, d)
					}
					m.log.Infof(`database "%v" sharding completed add to whitelist`, d)
					m.shardWhitelist[d] = d
					return nil
				} else {
					m.shardBlacklist[d] = d
					return errors.Errorf(`database "%v" not sharding add to blacklist`, d)
				}
			}
		}
	} else {
		m.shardBlacklist[d] = d
	}
	return errors.Errorf(`the result of checking the database "%v" sharding status is null`, d)
}

func (m *MongoDB) enableDBShared(d string) error {
	command := bson.D{{"enablesharding", d}}
	var result bson.M
	err := m.conn.Database("admin").RunCommand(context.Background(), command).Decode(&result)
	if err != nil {
		return errors.Errorf(`runCommand enablesharding "%v" failed: %v`, d, err.Error())
	} else {
		m.log.Infof(`runCommand enablesharding "%v" success: %v`, d, result)
	}

	return nil
}

func (m *MongoDB) CheckCollShard(d, c string) error {
	m.log.Infof(`check the collection "%v"."%v" sharding status`, d, c)

	// 首先判断集合是否存在
	if err := m.getCollExists(d, c); err != nil {
		return err
	}

	var result bson.M
	opts := options.FindOne().SetProjection(bson.M{"lastmod": 1, "key": 1, "unique": 1, "dropped": 1, "_id": 0})
	res := m.conn.Database("config").Collection("collections").FindOne(context.Background(), bson.D{{"_id", fmt.Sprintf("%v.%v", d, c)}}, opts)
	if err := res.Decode(&result); err != nil {
		if err == mongo.ErrNoDocuments {
			m.log.Infof(`the collection "%v"."%v" is not sharded and can be execute sharded`, d, c)
			return nil
		}
		return errors.Errorf(`check collection "%v"."%v" shared state error: %v`, d, c, err)
	}
	cache := make(map[string]interface{})
	for s, i := range result {
		cache[s] = i
	}
	if v, ok := cache["dropped"]; ok {
		if v.(bool) {
			// true 表示集合删除了
			m.log.Infof(`collection "%v"."%v" shard key already drop. can be execute sharding`, d, c)
			return nil
		} else {
			// false 表示没有删除集合
			// 如果存在key表示，集合已经分片。
			if v2, ok := cache["key"]; ok {
				output, _ := json.Marshal(v2)
				return errors.Errorf(`collection "%v"."%v" already sharded. sharding key: %v`, d, c, string(output))
			} else {
				m.log.Infof(`the collection "%v"."%v" is not sharded and can be execute sharded`, d, c)
				// 如果不存在key。表示集合未分片，那么就可以执行分片操作。
				return nil
			}
		}
	}

	return errors.Errorf(`check collection "%v"."%v" unknown error because the dropped column cannot be get`, d, c)
}

/*func (m *MongoDB) CheckColl(key *pargs.SoftBson) bool {
	err := m.CheckCollShard(key.EnterID, key.TableName)
	if err != nil {
		m.log.Errorf("check for sharding errors: %v", err)
		return false
	}
	output, _ := json.Marshal(b)
	m.log.Warnf(`"%v" is already sharded. sharding key: %s`, fmt.Sprintf("%v.%v", key.EnterID, key.TableName), output)
	return true
}*/

func (m *MongoDB) CheckDB(key *pargs.SoftBson) bool {
	m.log.Infof(`check "%v" database has shard state`, key.EnterID)
	err := m.CheckDBIsShard(key.EnterID)
	if err != nil {
		m.log.Warnf("%v", err)
		return true
	}
	return false
}

func (m *MongoDB) Shared(s []*pargs.SoftBson) {
	m.log.Infof("starting sharding for collection")
	for _, key := range s {
		// 检查该数据库是否跳过
		if m.CheckDB(key) {
			continue
		}

		// 检查集合是否已经被分片
		if err := m.CheckCollShard(key.EnterID, key.TableName); err != nil {
			m.log.Warnf("%v", err)
			continue
		}

		m.log.Infof(`starting the processing of "%v" sharding`, fmt.Sprintf("%v.%v", key.EnterID, key.TableName))

		opts := options.RunCmd().SetReadPreference(readpref.Primary())
		if len(key.KeyValues) != 1 {
			m.log.Errorf("partitioning key can only be one, not multiple, but can be a composite key: %v", key.KeyValues)
			continue
		}

		var sk bson.D
		for _, value := range key.KeyValues {
			switch v := value.(type) {
			case primitive.A:
				for _, i3 := range v {
					switch v2 := i3.(type) {
					case primitive.D:
						for _, e := range v2 {
							sk = append(sk, bson.E{Key: e.Key, Value: e.Value})
						}
					}
				}
			}
		}

		if len(sk) == 0 {
			m.log.Errorf("%v processing sharding key error. Shard key: %v", fmt.Sprintf("%v.%v", key.EnterID, key.TableName), key.KeyValues)
			continue
		}

		command := bson.D{{"shardCollection", fmt.Sprintf("%v.%v", key.EnterID, key.TableName)}, {"key", sk}}
		var result bson.M
		err := m.conn.Database("admin").RunCommand(context.Background(), command, opts).Decode(&result)
		if err != nil {
			m.log.Errorf(`runCommand shardCollection "%v" failed: %v`, fmt.Sprintf("%v.%v", key.EnterID, key.TableName), err.Error())
			continue
		}

		jsonData, err := json.Marshal(result)
		if err != nil {
			m.log.Infof(`enable sharding processing for "%v" completed: %v`, fmt.Sprintf("%v.%v", key.EnterID, key.TableName), result)
		} else {
			m.log.Infof(`enable sharding processing for "%v" completed: %v`, fmt.Sprintf("%v.%v", key.EnterID, key.TableName), string(jsonData))
		}
	}
	m.log.Infof("collection sharding has ended")
}

func (m *MongoDB) getInds(d, i string) ([]bson.M, error) {
	cursor, err := m.conn.Database(d).Collection(i).Indexes().List(context.Background())
	if err != nil {
		return nil, err
	}
	var resultAll []bson.M
	for cursor.Next(context.Background()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			return nil, err
		}
		resultAll = append(resultAll, result)

	}

	return resultAll, nil
}

func (m *MongoDB) getCollExists(d, c string) error {
	// 查看数据库缓存列表，是否存在
	if _, ok := m.dbNameList[d]; ok {
		if _, ok := m.dbNameList[d][c]; !ok {
			return errors.Errorf(`collection "%v"."%v" does not exist from name list cache`, d, c)
		}
	} else {
		m.log.Infof("load database %v name list to cache from database", d)
		// 如果缓存列表找不到该库，那么就去数据库中加载到缓存中。
		res, err := m.conn.Database(d).ListCollectionNames(context.Background(), bson.D{})
		if err != nil {
			m.dbNameList[d] = map[string]string{}
			return errors.Errorf(`loading "%v" tables from mongo database error: %v`, d, err)
		}
		m.log.Infof(`load a total of %v database "%v" table names`, len(res), d)
		if len(res) > 0 {
			// 如果从数据库提取到了数据，就放到缓存列表
			m.dbNameList[d] = map[string]string{}
			for _, re := range res {
				m.dbNameList[d][re] = re
			}

			// 从缓存列表检查是否存在该库表
			if _, ok := m.dbNameList[d][c]; !ok {
				return errors.Errorf(`collection "%v"."%v" does not exist from name list cache`, d, c)
			}
		} else {
			// 如果数据库中检索不到某个库下所有表名，则把库名放到缓存中，防止二次读库
			m.dbNameList[d] = map[string]string{}
			return errors.Errorf(`database "%v" does not exist or there are no tables under the database`, d)
		}
	}

	return nil
}

func (m *MongoDB) checkInds(idx *pargs.SoftBson) ([][]*Index, error) {
	m.log.Infof(`check if there are indexes in the collection "%v"."%v"`, idx.EnterID, idx.TableName)
	defer jxutils.ErrorCheckOfRecover(m.checkInds, m.log)
	cur, err := m.getInds(idx.EnterID, idx.TableName)
	if err != nil {
		return nil, errors.Errorf("get index error the %v", fmt.Sprintf("%v.%v", idx.EnterID, idx.TableName))
	}
	// 加载一存在的索引
	var existsIndex [][]*Index
	for _, res := range cur {
		v, ok := res["key"]
		if !ok {
			return nil, errors.Errorf("error in obtaining key values for all structures the %v", fmt.Sprintf("%v.%v", idx.EnterID, idx.TableName))
		}

		var ei []*Index
		if rv, ok := v.(primitive.M); ok {
			for key, typ := range rv {
				ei = append(ei, &Index{Key: key, IType: typ})
			}
		}
		existsIndex = append(existsIndex, ei)
	}

	// 加载参数文件中填写的索引
	var SetIndex [][]*Index
	for _, value := range idx.KeyValues {
		if v, ok := value.(primitive.A); ok {
			for _, i2 := range v {
				var si []*Index
				if vv, ok := i2.(primitive.D); ok {
					for _, e := range vv {
						si = append(si, &Index{Key: e.Key, IType: e.Value})
					}
				}
				SetIndex = append(SetIndex, si)
			}
		}

	}

	var resIndex [][]*Index
	for _, index := range SetIndex {
		if !m.indexVS(index, existsIndex) {
			resIndex = append(resIndex, index)
		}
	}
	return resIndex, nil
}

func (m *MongoDB) indexVS(ni []*Index, rawi [][]*Index) bool {
	defer jxutils.ErrorCheckOfRecover(m.indexVS, m.log)
	nl := len(ni)
	for _, indices := range rawi {
		if len(indices) == nl {
			var c int
			for i, index := range ni {
				if indices[i].Key == index.Key && ConvString(indices[i].IType) == ConvString(index.IType) {
					c++
					continue
				}
			}
			if c == nl {
				m.log.Warnf("index %v already exists in the collection. exists index: %v", CombinedInd(ni), CombinedInd(indices))
				return true
			}
		}
	}
	return false
}

func ConvString(i interface{}) string {
	var r string
	switch vv := i.(type) {
	case float32, float64:
		r = fmt.Sprintf("%.0f", vv)
	case int64:
		r = strconv.FormatInt(vv, 10)
	case int32:
		r = strconv.Itoa(int(vv))
	case int16:
		r = strconv.Itoa(int(vv))
	case int8:
		r = strconv.Itoa(int(vv))
	case int:
		r = strconv.Itoa(vv)
	case string:
		r = strings.TrimSpace(vv)
	}
	return r
}

func CombinedInd(ind []*Index) string {
	var s strings.Builder
	s.WriteString("{ ")
	for i, index := range ind {
		switch {
		case i == 0:
			s.WriteString(`"` + index.Key + `": `)
			switch v := index.IType.(type) {
			case string:
				s.WriteString(`"` + v + `"`)
			case int:
				s.WriteString(strconv.Itoa(v))
			case int32:
				s.WriteString(strconv.Itoa(int(v)))
			case int64:
				s.WriteString(strconv.Itoa(int(v)))
			case float32, float64:
				s.WriteString(fmt.Sprintf("%.0f", v))
			default:
				s.WriteString("UNKNOWN")
			}
		case i > 0:
			s.WriteString(`, "` + index.Key + `": `)
			switch v := index.IType.(type) {
			case string:
				s.WriteString(`"` + v + `"`)
			case int:
				s.WriteString(strconv.Itoa(v))
			case int32:
				s.WriteString(strconv.Itoa(int(v)))
			case int64:
				s.WriteString(strconv.Itoa(int(v)))
			case float32, float64:
				s.WriteString(fmt.Sprintf("%.0f", v))
			default:
				s.WriteString("UNKNOWN")
			}

		}
	}
	s.WriteString("}")
	return s.String()
}

func (m *MongoDB) CreateInd(indexes []*pargs.SoftBson) {
	m.log.Infof("start creating collection index")
	for _, ind := range indexes {
		// 检查集合是否存在
		if err := m.getCollExists(ind.EnterID, ind.TableName); err != nil {
			m.log.Warnf("skip index create due %v", err)
			continue
		}

		// 检查索引
		api, err := m.checkInds(ind)
		if err != nil {
			m.log.Warnf("%v", err)
			continue
		}
		if len(api) == 0 {
			m.log.Errorf(`all new indexes exist in the "%v"."%v" collection and do not need to be created`, ind.EnterID, ind.TableName)
			continue
		}

		m.log.Infof(`prepare the "%v"."%v" index structure: %v`, ind.EnterID, ind.TableName, ind.KeyValues)

		col := m.conn.Database(ind.EnterID).Collection(ind.TableName)

		// 准备索引结构
		var cim [][]mongo.IndexModel
		for _, indices := range api {
			var im []mongo.IndexModel
			var ibd bson.D
			for _, index := range indices {
				ibd = append(ibd, bson.E{Key: index.Key, Value: index.IType})
			}
			im = append(im, mongo.IndexModel{Keys: ibd})
			cim = append(cim, im)
		}
		if len(cim) > 0 {
			// 创建索引
			for _, models := range cim {
				ir, err := col.Indexes().CreateMany(context.Background(), models)
				if err != nil {
					m.log.Errorf(`error occurred while build the index for "%v"."%v": %v`, ind.EnterID, ind.TableName, err)
				}
				for _, indName := range ir {
					m.log.Infof(`successfully created index the "%v"."%v". index name is: %v`, ind.EnterID, ind.TableName, indName)
				}
			}
		} else {
			m.log.Errorf(`no index structure received for "%v"."%v"`, ind.EnterID, ind.TableName)
		}

	}
	m.log.Infof("create collection index end")
}

func (m *MongoDB) Close() {
	if m.conn != nil {
		m.log.Infof("close mongo connect")
		m.conn.Disconnect(context.Background())
	}
}
func NewMongo() *MongoDB {
	return new(MongoDB)
}
