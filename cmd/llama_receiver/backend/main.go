package main

import (
	"log"

	"main/config"
	"main/handlers"

	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New()

	config.Connect()

	app.Get("/get_database_list", handlers.GetAllDatabases)
	app.Get("/get_database_insight", handlers.GetAllInsights)

	log.Fatal(app.Listen(":6555"))
}
