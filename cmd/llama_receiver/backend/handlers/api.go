package handlers

import (
	"main/entities"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

func GetAllInsights(c *fiber.Ctx) error {
	insight_id, err := strconv.Atoi(c.Query("db_id"))

	if err != nil {
		return c.Status(500).JSON(err)
	}

	data, err := entities.GetAllInsightsForDB(c.Context(), insight_id)
	if err != nil {
		return c.Status(500).JSON("Internal Sevrer Error")
	}

	if len(data) == 0 {
		return c.Status(404).JSON("No records found")
	}

	return c.Status(200).JSON(&data)
}

func GetAllDatabases(c *fiber.Ctx) error {
	data, err := entities.GetDBs(c.Context())
	if err != nil {
		return c.Status(500).JSON("Internal Sevrer Error")
	}

	if len(data) == 0 {
		return c.Status(404).JSON("No records found")
	}

	return c.Status(200).JSON(data)
}
