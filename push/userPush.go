package push

import (
	"gosuc/go-timequeue/logger"
	"gosuc/go-timequeue/rediq"
)

type UserPushExpireHandler struct{}

func (handler *UserPushExpireHandler) HandleExpireData(data rediq.RediQElementCommon) error {
	logger.Log.Info("UserPushExpireHandler - key:", data.Key, " data:", data.Data, " age:", data.Age)
	return nil
}
