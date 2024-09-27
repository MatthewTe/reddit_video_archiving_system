package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Neo4JEnvironment struct {
	URI      string
	User     string
	Password string
}

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

func CoreInsertGraphData(requestContent []byte, env Neo4JEnvironment, ctx context.Context) (map[string]any, error) {

	var cypherEntities []map[string]interface{}
	if err := json.Unmarshal(requestContent, &cypherEntities); err != nil {
		log.Print("Error in unmarshaling full cypher request")
		return nil, err
	}

	driver, err := neo4j.NewDriverWithContext(env.URI, neo4j.BasicAuth(env.User, env.Password, ""))
	if err != nil {
		return nil, err
	}
	defer driver.Close(ctx)

	if err = driver.VerifyConnectivity(ctx); err != nil {
		return nil, err
	} else {
		log.Printf("Successfully connected to %s", env.URI)
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	graphResultMap, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		fullQueryResultMap := make(map[string]any)

		for _, cypherEntity := range cypherEntities {

			fullEntityBytes, _ := json.Marshal(cypherEntity)
			switch cypherEntity["type"].(string) {
			case "node":

				var node struct {
					CypherEntity
					NodeEntity
				}
				if err = json.Unmarshal(fullEntityBytes, &node); err != nil {
					log.Printf("Unable to unmarshal raw json query")
					return nil, err
				}

				// Constructing the raw cypher string for node:
				var formattedLabels string = strings.Join(node.Labels[:], ":")
				var nodeCypherQueryBuilder strings.Builder

				nodeCypherQueryBuilder.WriteString(fmt.Sprintf("%s (n:%s {\n", node.QueryType, formattedLabels))
				numProperties := len(node.Properties)
				count := 1
				for propertyName := range node.Properties {

					// Ad-Hoc validation and type wrapping (this will have to be moved as it grows in complexity)
					switch propertyName {

					case "published_date":
						if count == numProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", propertyName, propertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", propertyName, propertyName))
						}
					case "date":
						if count == numProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", propertyName, propertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", propertyName, propertyName))
						}
					case "datetime":
						if count == numProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s) \n", propertyName, propertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s), \n", propertyName, propertyName))
						}
					default:
						if count == numProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s \n", propertyName, propertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s, \n", propertyName, propertyName))
						}
					}
					count += 1
				}
				nodeCypherQueryBuilder.WriteString("})\nRETURN n")
				log.Print(nodeCypherQueryBuilder.String())

				nodeQueryResponse, err := tx.Run(ctx, nodeCypherQueryBuilder.String(), node.Properties)
				if err != nil {
					return nil, err
				}
				if nodeQueryResponse.Next(ctx) {
					fullQueryResultMap[node.Properties["id"].(string)] = nodeQueryResponse.Record().AsMap()
					log.Printf("Created node %s", node.Properties["id"].(string))
				}

			case "edge":

				var edge struct {
					CypherEntity
					EdgeEntity
				}

				if err = json.Unmarshal(fullEntityBytes, &edge); err != nil {
					return nil, err
				}

				formattedEdgeLabels := strings.Join(edge.Labels, ":")

				var edgeCypherQueryBuilder strings.Builder
				edgeCypherQueryBuilder.WriteString("MATCH (n {id: $from}), (m {id: $to})\n")

				if len(formattedEdgeLabels) == 0 {
					edgeCypherQueryBuilder.WriteString("MERGE (n)-[{\n\t")
				} else {
					edgeCypherQueryBuilder.WriteString(fmt.Sprintf("MERGE (n)-[c:%s {\n\t", formattedEdgeLabels))
				}

				numProperties := len(edge.Properties)
				count := 1
				for propertyName := range edge.Properties {

					switch propertyName {

					// Ad-hoc edge query type casting done manually - will have to move this when it gets to unweidly:
					case "date":
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: date($%s)", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: date($%s),", propertyName, propertyName))
						}
					case "datetime":
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: datetime($%s)", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: datetime($%s),", propertyName, propertyName))
						}

					default:
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: $%s", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: $%s,", propertyName, propertyName))
						}
					}
					count += 1
				}
				edgeCypherQueryBuilder.WriteString("}]->(m)\nRETURN c")
				log.Print(edgeCypherQueryBuilder.String())
				// Putting the to and from params in the properties map so I can pass it to the actualy cypher query:
				maps.Copy(edge.Properties, map[string]any{"to": edge.Connection.To, "from": edge.Connection.From})
				edgeQueryResponse, err := tx.Run(ctx, edgeCypherQueryBuilder.String(), edge.Properties)
				if err != nil {
					return nil, err
				}

				if edgeQueryResponse.Next(ctx) {
					var edgeConnLabel string = fmt.Sprintf("%s-%s", edge.Connection.From, edge.Connection.To)
					fullQueryResultMap[edgeConnLabel] = edgeQueryResponse.Record().AsMap()
					log.Printf("Created edge %s", edgeConnLabel)
				}
			}
		}
		return fullQueryResultMap, nil
	})

	if err != nil {
		return nil, err
	}

	return graphResultMap.(map[string]any), nil
}

type NodeExistsResponse struct {
	Id     string   `json:"id"`
	Exists bool     `json:"exists"`
	Labels []string `json:"node_labels"`
}

func CheckNodeExists(ids []string, env Neo4JEnvironment, ctx context.Context) ([]NodeExistsResponse, error) {

	driver, err := neo4j.NewDriverWithContext(env.URI, neo4j.BasicAuth(env.User, env.Password, ""))
	if err != nil {
		return nil, err
	}
	defer driver.Close(ctx)

	if err = driver.VerifyConnectivity(ctx); err != nil {
		return nil, err
	} else {
		log.Printf("Successfully connected to %s", env.URI)
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	nodeExistResponses, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		var nodeExistingResponses []NodeExistsResponse
		for _, id := range ids {

			idExistResponse, err := tx.Run(ctx,
				`
				OPTIONAL MATCH (n {id: $id})
				RETURN 
    				n IS NOT NULL AS exists, 
    				COALESCE(labels(n), []) AS node_labels
				`,
				map[string]any{"id": id},
			)
			if err != nil {
				return nil, err
			}

			if idExistResponse.Next(ctx) {
				idExistResponseMap := idExistResponse.Record().AsMap()

				nodeLabels := idExistResponseMap["node_labels"].([]interface{})
				var labels []string
				for _, value := range nodeLabels {
					labels = append(labels, value.(string))
				}

				nodeExistingResponses = append(nodeExistingResponses, NodeExistsResponse{
					Id:     id,
					Exists: idExistResponseMap["exists"].(bool),
					Labels: labels,
				})
			} else {
				log.Printf("Something strange happened when running the check-exists query for the %s node and was unable to append response", id)
			}
		}
		return nodeExistingResponses, nil
	})

	if err != nil {
		return nil, err
	}

	return nodeExistResponses.([]NodeExistsResponse), nil

}
