package main

import (
	"context"

	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func initContainer(ctx context.Context) (*localstack.LocalStackContainer, error) {
	localstackContainer, err := localstack.Run(ctx, "localstack/localstack:1.4.0")

	if err != nil {
		return nil, err
	}

	return localstackContainer, nil
}
