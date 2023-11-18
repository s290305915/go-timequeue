package router

import (
	"gosuc/go-timequeue/handlers"

	"github.com/gin-gonic/gin"
)

func InitRouters() *gin.Engine {
	g := gin.New()

	g.Use(gin.Logger())
	g.Use(gin.Recovery())

	g.GET("/", handlers.Index)
	g.GET("/getall", handlers.GetAllSchedule)
	g.POST("/put", handlers.PutSchedule)
	g.POST("/cancel", handlers.CancelSchedule)

	return g
}
