package config

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type H3Cell struct {
	Index string
}

func InsertSpatialIndexNodes(h3Cells []H3Cell, env Neo4JEnvironment, ctx context.Context) ([]H3Cell, error) {

	driver, err := neo4j.NewDriverWithContext(
		env.URI,
		neo4j.BasicAuth(env.User, env.Password, ""),
	)
	if err != nil {
		panic(err)
	}

	defer driver.Close(ctx)

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		panic(err)
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	createdCellsResult, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			var createdCells []H3Cell

			for _, cell := range h3Cells {
				createdCellResult, err := tx.Run(
					ctx,
					"MERGE (h3Index:H3Index:H3Cell:Spatial:Settings {index: $index}) RETURN h3Index.index AS h3_index",
					map[string]any{"index": cell.Index},
				)
				if err != nil {
					return nil, err
				}

				createdCellResponse, err := createdCellResult.Single(ctx)
				if err != nil {
					return nil, err
				}

				createdCells = append(createdCells, H3Cell{
					Index: createdCellResponse.AsMap()["h3_index"].(string),
				})
			}

			return createdCells, err

		})

	if err != nil {
		return nil, err
	}

	return createdCellsResult.([]H3Cell), nil
}

func InsertDatetimeNode() {}
