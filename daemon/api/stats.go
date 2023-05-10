package api

import (
	"github.com/application-research/delta-importer/db"
	"github.com/labstack/echo/v4"
)

func ConfigureStatsRouter(e *echo.Group, db *db.DIDB) {
	stats := e.Group("stats")

	stats.GET("", func(c echo.Context) error {
		ds, err := db.GetDealStats()

		if err != nil {
			return err
		}

		return c.JSON(200, ds)
	})
}
