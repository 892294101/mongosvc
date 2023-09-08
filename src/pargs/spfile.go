package pargs

import (
	"github.com/892294101/jxutils"
	"github.com/magiconair/properties"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	DBConnAddressKey = "ADDRESS"
	DBConnUrlKey     = "URL"
	DBConnRegexp     = `^(?i)` + DBConnAddressKey + `\.(?i)(` + DBConnUrlKey + `)$`

	IndexKey    = "INDEX"
	IndexRegexp = `^(?i)` + IndexKey + `\.([a-zA-Z0-9_\-]+)\.([a-zA-Z0-9_\-]+)$`

	ShardKey    = "SHARD"
	ShardRegexp = `^(?i)` + ShardKey + `\.([a-zA-Z0-9_\-]+)\.([a-zA-Z0-9_\-]+)$`

	EnableShardKey    = "ENABLESHARDING"
	EnableShardRegexp = `^(?i)` + EnableShardKey + `$`
)

type KeyLibs interface {
	init(p *properties.Properties) error
	verification(p *properties.Properties, kv []string) error
}

type Address struct {
	keySets map[string]string
	p       *properties.Properties
}

func (a *Address) init(p *properties.Properties) error {
	if a.keySets == nil {
		a.keySets = make(map[string]string)
	}
	if a.p == nil {
		a.p = p
	}
	return nil
}

func (a *Address) verification(p *properties.Properties, kv []string) error {
	a.init(p)
	val, _ := a.p.Get(kv[0]) // get value from parameter file
	url := strings.ToUpper(kv[1])
	a.keySets[url] = strings.TrimSpace(val)
	return nil
}

type SoftBson struct {
	EnterID   string
	TableName string
	KeyValues []interface{}
}

type Indexes struct {
	keySets map[string]map[string][]interface{}
	softKey []*SoftBson
	p       *properties.Properties
}

func (i *Indexes) init(p *properties.Properties) error {
	if i.keySets == nil {
		i.keySets = make(map[string]map[string][]interface{})
	}
	if i.p == nil {
		i.p = p
	}
	return nil
}

func (i *Indexes) verification(p *properties.Properties, kv []string) error {
	i.init(p)

	val, _ := i.p.Get(kv[0]) // get value from parameter file
	keyParam := kv[0]
	dbname := kv[1]
	table := kv[2]
	indRes, err := jxutils.JsonStr2Bson(val)
	if err != nil {
		return errors.Errorf("value of keyword %v must be a Bson structure: %v", keyParam, val)
	}

	if _, ok := i.keySets[dbname]; !ok {
		i.keySets[dbname] = make(map[string][]interface{})
		i.keySets[dbname][table] = append(i.keySets[dbname][table], indRes)
	} else {
		i.keySets[dbname][table] = append(i.keySets[dbname][table], indRes)
	}
	// 添加排序索引键
	i.softKey = append(i.softKey, &SoftBson{EnterID: dbname, TableName: table})
	return nil
}

type Shard struct {
	keySets map[string]map[string][]interface{}
	softKey []*SoftBson
	p       *properties.Properties
}

func (s *Shard) init(p *properties.Properties) error {
	if s.keySets == nil {
		s.keySets = make(map[string]map[string][]interface{})
	}
	if s.p == nil {
		s.p = p
	}
	return nil
}

func (s *Shard) verification(p *properties.Properties, kv []string) error {
	s.init(p)

	val, _ := s.p.Get(kv[0]) // get value from parameter file
	keyParam := kv[0]
	dbname := kv[1]
	table := kv[2]
	sRes, err := jxutils.JsonStr2Bson(val)
	if err != nil {
		return errors.Errorf("value of keyword %v must be a Bson structure: %v", keyParam, val)
	}

	if _, ok := s.keySets[dbname]; !ok {
		s.keySets[dbname] = make(map[string][]interface{})
		s.keySets[dbname][table] = append(s.keySets[dbname][table], sRes)
	} else {
		s.keySets[dbname][table] = append(s.keySets[dbname][table], sRes)
	}
	s.softKey = append(s.softKey, &SoftBson{EnterID: dbname, TableName: table})
	return nil
}

type EnableShard struct {
	keySets bool
	p       *properties.Properties
}

func (e *EnableShard) init(p *properties.Properties) error {
	if e.p == nil {
		e.p = p
	}
	return nil
}

func (e *EnableShard) verification(p *properties.Properties, kv []string) error {
	if err := e.init(p); err != nil {
		return err
	}

	v, ok := e.p.Get(kv[0])
	if ok {
		if len(v) > 0 {
			return errors.Errorf("parameter %v cannot be set to any value", kv[0])
		} else {
			e.keySets = true
		}
	}
	return nil
}

type Param struct {
	kls     map[string]interface{}
	keyLibs map[string]string
	log     *logrus.Logger
}

func (s *Param) regKeyLibs() {
	s.registerKey(DBConnRegexp)
	s.registerKey(IndexRegexp)
	s.registerKey(ShardRegexp)
	s.registerKey(EnableShardRegexp)
	s.log.Infof("Registered %v keywords", len(s.keyLibs))
}

func (s *Param) SetLogger(log *logrus.Logger) {
	s.log = log
}
func (s *Param) Load() error {
	dir, err := jxutils.GetProgramHome()
	if err != nil {
		return err
	}
	configPath := filepath.Join(dir, "conf", "svc.conf")
	s.log.Infof("read %v configuration file content", configPath)
	p, err := properties.LoadFile(configPath, properties.UTF8)
	if err != nil {
		return err
	}

	s.kls = make(map[string]interface{})
	s.regKeyLibs()

	for _, kk := range p.Keys() {
		var illegal bool
		for keyRegx, _ := range s.keyLibs {
			re := regexp.MustCompile(keyRegx)
			kv := re.FindStringSubmatch(kk)
			if re.MatchString(kk) {
				illegal = true
				switch keyRegx {
				case DBConnRegexp:
					v, ok := s.kls[DBConnAddressKey]
					if !ok {
						var kl KeyLibs
						kl = &Address{}
						if err := kl.verification(p, kv); err != nil {
							return err
						}
						s.kls[DBConnAddressKey] = kl
					} else {
						if add, ok := v.(*Address); ok {
							if err := add.verification(p, kv); err != nil {
								return err
							}
						}
					}
					goto DoneRegx
				case IndexRegexp:
					v, ok := s.kls[IndexKey]
					if !ok {
						var kl KeyLibs
						kl = &Indexes{}
						if err := kl.verification(p, kv); err != nil {
							return err
						}
						s.kls[IndexKey] = kl
					} else {
						if idx, ok := v.(*Indexes); ok {
							if err := idx.verification(p, kv); err != nil {
								return err
							}
						}
					}
					goto DoneRegx
				case ShardRegexp:
					v, ok := s.kls[ShardKey]
					if !ok {
						var sl KeyLibs
						sl = &Shard{}
						if err := sl.verification(p, kv); err != nil {
							return err
						}
						s.kls[ShardKey] = sl
					} else {
						if sdx, ok := v.(*Shard); ok {
							if err := sdx.verification(p, kv); err != nil {
								return err
							}
						}
					}
					goto DoneRegx
				case EnableShardRegexp:
					_, ok := s.kls[EnableShardKey]
					if !ok {
						var es KeyLibs
						es = &EnableShard{}
						if err := es.verification(p, kv); err != nil {
							return err
						}
						s.kls[EnableShardKey] = es
					}
					goto DoneRegx
				}
			}

		}
		if !illegal {
			return errors.Errorf("Keyword illegal: %v", kk)
		}
	DoneRegx:
	}

	return s.softKeyVale()
}

func (s *Param) softKeyVale() error {
	v, ok := s.kls[IndexKey]
	if ok {
		if iv, ok := v.(*Indexes); ok {
			for _, index := range iv.softKey {
				data, ok := iv.keySets[index.EnterID][index.TableName]
				if ok {
					index.KeyValues = data
				} else {
					return errors.Errorf("found non indexed structure during index key sorting: database: %v. table: %v", index.EnterID, index.TableName)
				}
			}
		}
	}

	v, ok = s.kls[ShardKey]
	if ok {
		if iv, ok := v.(*Shard); ok {
			for _, index := range iv.softKey {
				data, ok := iv.keySets[index.EnterID][index.TableName]
				if ok {
					index.KeyValues = data
				} else {
					return errors.Errorf("found non shard key structure during key sorting: database: %v. table: %v", index.EnterID, index.TableName)
				}
			}
		}
	}

	return nil
}

func (s *Param) GetUrl() (string, error) {
	v, ok := s.kls[DBConnAddressKey]
	if ok {
		if av, ok := v.(*Address); ok {
			return av.keySets[DBConnUrlKey], nil
		}
	}
	return "", errors.Errorf("unable to find Address parameter")
}

func (s *Param) GetInds() ([]*SoftBson, error) {
	s.log.Infof("read index building parameter data")
	v, ok := s.kls[IndexKey]
	if ok {
		if iv, ok := v.(*Indexes); ok {
			return iv.softKey, nil
		}
	}
	return nil, errors.Errorf("unable to find Index parameter")
}

func (s *Param) GetShared() ([]*SoftBson, error) {
	s.log.Infof("read shared building parameter data")
	v, ok := s.kls[ShardKey]
	if ok {
		if sv, ok := v.(*Shard); ok {
			return sv.softKey, nil
		}
	}
	return nil, errors.Errorf("unable to find shard parameter")
}

func (s *Param) GetEnShard() bool {
	_, ok := s.kls[EnableShardKey]
	if ok {
		return true
	}
	return false
}

func (s *Param) registerKey(key string) error {
	if s.keyLibs == nil {
		s.keyLibs = make(map[string]string)
	}

	if _, ok := s.keyLibs[key]; ok {
		return errors.Errorf("Keyword regular already exists: %v", key)
	}
	s.keyLibs[key] = ""
	return nil
}

func NewParams() *Param {
	return new(Param)
}
