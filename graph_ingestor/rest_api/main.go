package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type CypherEntity struct {
	Type      string   `json:"type"`
	QueryType string   `json:"query_type"`
	Labels    []string `json:"labels"`
}

type NodeEntity struct {
	Properties map[string]any `json:"properties"`
}
type EdgeConnection struct {
	To   string `json:"to"`
	From string `json:"from"`
}
type EdgeEntity struct {
	Connection EdgeConnection `json:"connection"`
	Properties map[string]any `json:"properties"`
}

func IngestGraphJSON(jsonBytes []byte) {

	var cypherEntities []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &cypherEntities); err != nil {
		log.Fatal(err)
	}

	var env config.Neo4JEnvironment = config.Neo4JEnvironment{
		URI:      "neo4j://localhost:7687",
		User:     "neo4j",
		Password: "test_password",
	}
	var ctx context.Context = context.Background()

	driver, err := neo4j.NewDriverWithContext(
		env.URI,
		neo4j.BasicAuth(env.User, env.Password, ""))
	if err != nil {
		log.Fatal(err)
	}
	defer driver.Close(ctx)
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection Established")

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	_, err = session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		for _, cypherEntity := range cypherEntities {
			entityType := cypherEntity["type"].(string)

			switch entityType {
			case "node":
				var node struct {
					CypherEntity
					NodeEntity
				}

				nodeBytes, _ := json.Marshal(cypherEntity)
				if err = json.Unmarshal(nodeBytes, &node); err != nil {
					log.Fatal(err)
				}

				var formattedLabels string = strings.Join(node.Labels[:], ":")
				var sb strings.Builder

				sb.WriteString(fmt.Sprintf("%s (n:%s {\n", node.QueryType, formattedLabels))
				numProperties := len(node.Properties)
				count := 1
				for propertyName, _ := range node.Properties {

					fmt.Println(numProperties, count)
					switch propertyName {
					case "published_date":
						if count == numProperties {
							sb.WriteString(fmt.Sprintf("\t %s: date($%s) \n", propertyName, propertyName))
						} else {
							sb.WriteString(fmt.Sprintf("\t %s: date($%s), \n", propertyName, propertyName))
						}
					case "date":
						if count == numProperties {
							sb.WriteString(fmt.Sprintf("\t %s: date($%s) \n", propertyName, propertyName))
						} else {
							sb.WriteString(fmt.Sprintf("\t %s: date($%s), \n", propertyName, propertyName))
						}
					default:
						if count == numProperties {
							sb.WriteString(fmt.Sprintf("\t %s: $%s \n", propertyName, propertyName))
						} else {
							sb.WriteString(fmt.Sprintf("\t %s: $%s, \n", propertyName, propertyName))
						}
					}
					count += 1
				}
				sb.WriteString("})\nRETURN n")
				fmt.Println(sb.String())
				queryResponse, err := tx.Run(ctx, sb.String(), node.Properties)
				if err != nil {
					log.Fatal(err)
				}
				if queryResponse.Next(ctx) {
					resultNodeMap := queryResponse.Record().AsMap()
					fmt.Println(resultNodeMap)
				}

			case "edge":
				var edge struct {
					CypherEntity
					EdgeEntity
				}

				edgeBytes, _ := json.Marshal(cypherEntity)
				if err = json.Unmarshal(edgeBytes, &edge); err != nil {
					log.Fatal(err)
				}

				formattedEdgeLabels := strings.Join(edge.Labels, ":")
				fmt.Println(formattedEdgeLabels)

				var sb strings.Builder
				sb.WriteString("MATCH (n {id: $from}), (m {id: $to})\n")

				if len(formattedEdgeLabels) == 0 {
					sb.WriteString("CREATE (n)-[{\n\t")
				} else {
					sb.WriteString(fmt.Sprintf("CREATE (n)-[c:%s {\n\t", formattedEdgeLabels))
				}

				numProperties := len(edge.Properties)
				count := 1
				for propertyName, _ := range edge.Properties {

					switch propertyName {

					case "date":
						if count == numProperties {
							sb.WriteString(fmt.Sprintf("%s: date($%s)", propertyName, propertyName))
						} else {
							sb.WriteString(fmt.Sprintf("%s: date($%s),", propertyName, propertyName))
						}

					default:
						if count == numProperties {
							sb.WriteString(fmt.Sprintf("%s: $%s", propertyName, propertyName))
						} else {
							sb.WriteString(fmt.Sprintf("%s: $%s,", propertyName, propertyName))
						}
					}
					count += 1
				}
				sb.WriteString("}]->(m)\nRETURN c")

				maps.Copy(edge.Properties, map[string]any{"to": edge.Connection.To, "from": edge.Connection.From})
				edgeResponse, err := tx.Run(ctx, sb.String(), edge.Properties)
				if err != nil {
					log.Fatal(err)
				}
				if edgeResponse.Next(ctx) {
					resultNodeMap := edgeResponse.Record().AsMap()
					fmt.Println(resultNodeMap)
				}

			}

		}

		return nil, nil

	})
	if err != nil {
		log.Fatal(err)
	}

}

func ValidateNodeViaLabel(propertiesMap map[string]any, label string) map[string]any {
	switch label {
	case "Post":
		var propertyLabels []string
		for property, _ := range propertiesMap {
			propertyLabels = append(propertyLabels, property)
		}

		containsId := slices.Contains(propertyLabels, "id")
		containsPublishedDate := slices.Contains(propertyLabels, "published_date")
		_, parsePublishedDateError := time.Parse("2006-01-02", propertiesMap["published_date"].(string))

		return map[string]any{
			"id":                        containsId,
			"published_date":            containsPublishedDate,
			"published_date_format_err": parsePublishedDateError,
		}
	default:
		return map[string]any{
			"msg": fmt.Sprintf("No formatting supported for label %s", label),
		}
	}
}

func main() {

	const input = `

	[{
        "type":"node",
        "query_type": "CREATE",
        "labels": ["Entity", "Post", "Reddit"],
        "properties": {
            "id":"2b0a79f0-59eb-4ac8-b880-c7887de130cd",
            "url": "https://www.reddit.com/r/CombatFootage/comments/1fm8edl/israeli_air_strike_in_lebanon_causes_massive/",
            "title": "Israeli air strike in Lebanon causes massive shockwave",
            "static_root_url":"2b0a79f0-59eb-4ac8-b880-c7887de130cd/",
            "published_date": "2024-09-21",
            "static_downloaded": false,
            "static_file_type": null
        }
    },
    {
        "type":"node",
        "query_type":"CREATE",
        "labels": ["Date"],
        "properties": {
            "id":"227eb496-b67a-45d8-8654-ddeff268422a",
            "day":"2024-09-21"
        }
    },
    {
        "type":"edge",
		"connection": {
		    "from": "2b0a79f0-59eb-4ac8-b880-c7887de130cd",
            "to": "227eb496-b67a-45d8-8654-ddeff268422a"
		},
		"labels": ["EXTRACTED"],
        "properties": {
            "date": "2024-09-21"
        }
    }]

	`

	var cypherEntity CypherEntity
	buf := []byte(input)
	IngestGraphJSON(buf)

	os.Exit(0)

	if err := json.Unmarshal(buf, &cypherEntity); err != nil {
		log.Fatal(err)
	}

	switch cypherEntity.Type {
	case "node":
		var node struct {
			CypherEntity
			NodeEntity
		}
		if err := json.Unmarshal(buf, &node); err != nil {
			log.Fatal(err)
		}

		var valdiationResults map[string]any = map[string]any{}
		for _, label := range node.Labels {
			propertiesValidationMap := ValidateNodeViaLabel(node.Properties, label)
			valdiationResults[label] = propertiesValidationMap
		}

		fmt.Println(valdiationResults)

		// For testing assume that everything has beeen correctly validated:
	}

}
