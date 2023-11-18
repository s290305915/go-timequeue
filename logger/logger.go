package logger

import (
	"fmt"
	"go.uber.org/zap"
)

var Log *zap.SugaredLogger

func getSugarFields(fields map[string]interface{}) []interface{} {
	args := make([]interface{}, 0, len(fields)*2)
	for key, value := range fields {
		args = append(args, key, value)
	}
	return args
}

// Child create child logger of Log
func Child(fields map[string]interface{}) (log *zap.SugaredLogger) {
	childLogs := Log.With(getSugarFields(fields)...)
	return childLogs
}

// Init for over gin setting
func Init(logLevel string) {
	if logLevel == "" {
		logLevel = "debug"
	}

	logger, _ := zap.NewDevelopment()

	//logger, _ := zap.NewProduction()
	// if pkg.APPENV != "production" {
	// 	logger, _ = zap.NewDevelopment()
	// }
	defer logger.Sync() // flushes buffer, if any
	log := logger.Sugar()

	Log = log
}

// MyStdLogger 用于mysql的日志输出
type MyStdLogger struct {
}

func (m *MyStdLogger) Print(v ...interface{}) {
	Log.Info(v...)

}

func (m *MyStdLogger) Printf(format string, v ...interface{}) {

	Log.Info(fmt.Sprintf(format, v...))

}
func (m *MyStdLogger) Println(v ...interface{}) {
	Log.Info(v...)
}

func (m *MyStdLogger) Info(format string, v ...interface{}) {
	Log.Info(fmt.Sprintf(format, v...))

}
func (m *MyStdLogger) Debug(format string, v ...interface{}) {
	Log.Debug(fmt.Sprintf(format, v...))

}

func (m *MyStdLogger) Warn(format string, v ...interface{}) {
	Log.Warn(fmt.Sprintf(format, v...))

}

func (m *MyStdLogger) Error(format string, v ...interface{}) {
	Log.Error(fmt.Sprintf(format, v...))
}
