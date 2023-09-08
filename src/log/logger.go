package mlog

import (
	"fmt"
	"github.com/892294101/jxutils"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// 自义定日志结构
type myFormatter struct {
	groupid string
}

const (
	LogName = "svc"
)

func (s *myFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := time.Now().Local().Format("2006-01-02 15:04:05.00")
	var reason interface{}
	if v, ok := entry.Data["err"]; ok {
		reason = v
	} else {
		reason = nil
	}
	var msg string
	fName := entry.Caller.Function[strings.LastIndex(entry.Caller.Function, ".")+1:]
	if reason == nil {
		msg = fmt.Sprintf("%s %s %14s %04v [M] [%s] %s\n", timestamp, strings.ToUpper(entry.Level.String())[:1], fName, entry.Caller.Line, s.groupid, entry.Message)
	} else {
		msg = fmt.Sprintf("%s %s %14s %04d [M] [%s] %s %v\n", timestamp, strings.ToUpper(entry.Level.String())[:1], fName, entry.Caller.Line, s.groupid, entry.Message, reason)
	}
	return []byte(msg), nil
}

func (s *myFormatter) SetGroupId(groupid string) {
	s.groupid = groupid
}

// 初始化日志输出
// 定义为同事输出日志内容到标准输出和和日志文件
func InitDDSlog(groupid string) (*logrus.Logger, error) {
	ddslog := logrus.New()

	dir, err := jxutils.GetProgramHome()
	if err != nil {
		return nil, err
	}

	log := filepath.Join(dir, "logs", fmt.Sprintf("%s.log", strings.ToLower(LogName)))
	logfile, err := os.OpenFile(log, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	//writers := []io.Writer{logfile, os.Stdout}
	writers := []io.Writer{logfile}
	fileAndStdoutWriter := io.MultiWriter(writers...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Log file open failed: %s", err)
		return nil, err
	} else {
		ddslog.SetOutput(fileAndStdoutWriter)
	}
	ddslog.SetLevel(logrus.InfoLevel)
	ddslog.SetReportCaller(true)

	delog := new(myFormatter)
	delog.SetGroupId(groupid)
	ddslog.SetFormatter(delog)
	//ddslog.Infof("Initialize log file: %s", log)
	return ddslog, nil
}
