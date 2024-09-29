package main

import (
	"log"

	"main/config"

	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New()

	config.Connect()

	// app.Get("/get_database_list", handlers.GetAllApis)
	// app.Get("/get_database_insight", handlers.GetAllApis)

	log.Fatal(app.Listen(":6555"))
}
