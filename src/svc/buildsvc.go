package svc

import (
	mlog "github.com/892294101/mongosvc/src/log"
	"github.com/892294101/mongosvc/src/mdb"
	"github.com/892294101/mongosvc/src/pargs"
	"github.com/sirupsen/logrus"
	_ "net/http/pprof"
)

type BuildSvc struct {
	db  *mdb.MongoDB
	sp  *pargs.Param
	log *logrus.Logger
}

func (b *BuildSvc) build() {
	b.log.Infof("start building database collection indexes")
	ind, err := b.sp.GetInds()
	if err != nil {
		b.log.Errorf("%v", err)
	} else {
		b.db.CreateInd(ind)
	}

	b.log.Infof("start building database collection sharding")
	sha, err := b.sp.GetShared()
	if err != nil {
		b.log.Errorf("%v", err)
	} else {
		if len(sha) > 0 {
			b.db.Shared(sha)
		}
	}

}

func (b *BuildSvc) setLogger(log *logrus.Logger) {
	b.log = log
}

func (b *BuildSvc) Load() error {
	// 初始化log
	log, err := mlog.InitDDSlog("MONGOSVC")
	if err != nil {
		return err
	}
	b.setLogger(log)
	// 初始化参数
	p := pargs.NewParams()
	p.SetLogger(log)
	err = p.Load()
	if err != nil {
		log.Errorf("%v", err)
		return nil
	}
	b.sp = p

	// 获取mongo url
	add, err := b.sp.GetUrl()
	if err != nil {
		log.Errorf("%v", err)
		return nil
	}

	db := mdb.NewMongo()
	db.SetLogger(log)
	db.SetLoggerParam(b.sp)
	if err := db.InitMongo(add); err != nil {
		log.Errorf("%v", err)
		return nil
	}
	defer db.Close()
	b.db = db
	b.build()
	return nil
}

func NewBuildSvc() *BuildSvc {
	return new(BuildSvc)
}
