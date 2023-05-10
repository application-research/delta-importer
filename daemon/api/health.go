package api

import (
	"github.com/labstack/echo/v4"
)

// Health check routes for verifying API is alive
func ConfigureHealthRouter(e *echo.Group) {
	health := e.Group("/health")

	health.GET("", func(c echo.Context) error {

		resp := "alive"

		return c.JSON(200, resp)
	})
}
