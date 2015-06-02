package main

import (
	"bufio"
	"fmt"
	"github.com/bmizerany/lpx"
	"github.com/gin-gonic/gin"
	"github.com/srid/drain"
	"log"
	"net/http"
)

func main() {
	var routerGroup *gin.RouterGroup
	router := gin.Default()

	username, password, authorize := config.GetUserPass()
	if authorize {
		routerGroup = router.Group("/",
			gin.BasicAuth(gin.Accounts{username: password}))
	} else {
		routerGroup = router.Group("/")
	}

	routerGroup.POST("/logs", logsReceived)
	log.Printf("Running drain server at port %v\b", config.Port)
	router.Run(":" + config.Port)
}

func logsReceived(c *gin.Context) {
	defer c.Request.Body.Close()
	handleLog(c.Request)
	c.JSON(200, gin.H{})
}

func handleLog(r *http.Request) {
	lp := lpx.NewReader(bufio.NewReader(r.Body))
	for lp.Next() {
		fmt.Printf("[LOG] %s", drain.Record{lp.Header(), lp.Bytes()}.String())
	}
}
