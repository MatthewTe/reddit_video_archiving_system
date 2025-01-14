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

					case "geometry":

						if count == numProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: point({latitude: toFloat($latitude), longitude: toFloat($longitude)}) \n", propertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: point({latitude: toFloat($latitude), longitude: toFloat($longitude)}), \n", propertyName))
						}

						// Flattening the geometry info by adding it to the properties map directly instead of nested dict:
						node.Properties["latitude"] = node.Properties[propertyName].(map[string]interface{})["latitude"]
						node.Properties["longitude"] = node.Properties[propertyName].(map[string]interface{})["longitude"]

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
				fmt.Print(nodeCypherQueryBuilder.String())

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
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: date($%s) ", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: date($%s), ", propertyName, propertyName))
						}
					case "datetime":
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: datetime($%s) ", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: datetime($%s), ", propertyName, propertyName))
						}

					case "geometry":
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: point({latitude: toFloat($latitude), longitude: toFloat($longitude)}) \n", propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: point({latitude: toFloat($latitude), longitude: toFloat($longitude)}), \n", propertyName))
						}

						// Flattening the geometry info by adding it to the properties map directly instead of nested dict:
						edge.Properties["latitude"] = edge.Properties[propertyName].(map[string]interface{})["latitude"]
						edge.Properties["longitude"] = edge.Properties[propertyName].(map[string]interface{})["longitude"]

					default:
						if count == numProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: $%s ", propertyName, propertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("%s: $%s, ", propertyName, propertyName))
						}

					}
					count += 1
				}
				edgeCypherQueryBuilder.WriteString("}]->(m)\nRETURN c")
				fmt.Print(edgeCypherQueryBuilder.String())

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

type UpdateNodeEntity struct {
	MatchProperties map[string]any `json:"match_properties"`
	SetProperties   map[string]any `json:"set_properties"`
}
type UpdateEdgeEntity struct {
	Connection      EdgeConnection `json:"connection"`
	MatchProperties map[string]any `json:"match_properties"`
	SetProperties   map[string]any `json:"set_properties"`
}

func CoreUpdateGraphData(requestContent []byte, env Neo4JEnvironment, ctx context.Context) ([]map[string]any, error) {

	// Unmarshaling the json into an array of nodes or edges.
	var cypherEntities []map[string]interface{}
	if err := json.Unmarshal(requestContent, &cypherEntities); err != nil {
		log.Print("Error in unmarshaling full cypher request")
	}

	// Create the neo4j connection and the execute session.
	driver, err := neo4j.NewDriverWithContext(env.URI, neo4j.BasicAuth(env.User, env.Password, ""))
	if err != nil {
		return nil, err
	}
	defer driver.Close(ctx)

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	graphResultMap, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {

		// Loop through each of the json items in the session. If its node construct query. If its edge
		var fullQueryResult []map[string]any

		for _, cypherEntity := range cypherEntities {

			fullEntityBytes, _ := json.Marshal(cypherEntity)
			switch cypherEntity["type"].(string) {
			case "node":

				var node struct {
					CypherEntity
					UpdateNodeEntity
				}

				if err = json.Unmarshal(fullEntityBytes, &node); err != nil {
					return nil, err
				}

				var formattedLabels string = strings.Join(node.Labels[:], ":")
				var nodeCypherQueryBuilder strings.Builder

				nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\n%s (n:%s {\n", "MATCH", formattedLabels))
				numMatchProperties := len(node.MatchProperties)
				count := 1

				// Copy and rename the match property map to have a match prefix:
				remappedMatchProperties := make(map[string]any)
				for matchKey := range node.MatchProperties {
					remappedMatchProperties[matchKey] = fmt.Sprintf("match_%s", matchKey)
				}

				for matchPropertyName, prefixedPropertyName := range remappedMatchProperties {

					switch matchPropertyName {
					case "date":
						if count == numMatchProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", matchPropertyName, prefixedPropertyName))
						}

					case "published_date":
						if count == numMatchProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", matchPropertyName, prefixedPropertyName))
						}

					case "datetime":
						if count == numMatchProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s), \n", matchPropertyName, prefixedPropertyName))
						}

					default:
						if count == numMatchProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s \n", matchPropertyName, prefixedPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s, \n", matchPropertyName, prefixedPropertyName))
						}

					}
					count += 1
				}

				nodeCypherQueryBuilder.WriteString("})\n SET \n")

				count = 1

				remappedSetPropertes := make(map[string]any)
				for SetPropertyName := range node.SetProperties {
					remappedSetPropertes[SetPropertyName] = fmt.Sprintf("set_%s", SetPropertyName)
				}

				numSetProperties := len(node.SetProperties)
				for setPropertyName, prefixedSetPropertyName := range remappedSetPropertes {

					switch setPropertyName {

					case "date":
						if count == numSetProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = date($%s) \n", setPropertyName, prefixedSetPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = date($%s), \n", setPropertyName, prefixedSetPropertyName))
						}
					case "published_date":
						if count == numSetProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = date($%s) \n", setPropertyName, prefixedSetPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = date($%s), \n", setPropertyName, prefixedSetPropertyName))
						}

					case "datetime":
						if count == numSetProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = datetime($%s) \n", setPropertyName, prefixedSetPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = datetime($%s), \n", setPropertyName, prefixedSetPropertyName))
						}

					default:
						if count == numSetProperties {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = $%s \n", setPropertyName, prefixedSetPropertyName))
						} else {
							nodeCypherQueryBuilder.WriteString(fmt.Sprintf("\t n.%s = $%s, \n", setPropertyName, prefixedSetPropertyName))
						}
					}

					count += 1
				}

				nodeCypherQueryBuilder.WriteString("RETURN n")

				// Merging all of the property (match and set) maps:
				totalPropertyMap := make(map[string]any)
				for matchPropertyName, prefixedPropertyName := range remappedMatchProperties {
					totalPropertyMap[prefixedPropertyName.(string)] = node.MatchProperties[matchPropertyName]
				}
				for setPropertyName, prefixedSetPropertyName := range remappedSetPropertes {
					totalPropertyMap[prefixedSetPropertyName.(string)] = node.SetProperties[setPropertyName]
				}

				nodeQueryResponse, err := tx.Run(ctx, nodeCypherQueryBuilder.String(), totalPropertyMap)
				if err != nil {
					return nil, err
				}
				if nodeQueryResponse.Next(ctx) {
					fullQueryResult = append(fullQueryResult, nodeQueryResponse.Record().AsMap())
					log.Printf("Updated node")
				}

			case "edge":
				var edge struct {
					CypherEntity
					UpdateEdgeEntity
				}

				if err = json.Unmarshal(fullEntityBytes, &edge); err != nil {
					return nil, err
				}

				/*
					CREATE (n:Reddit {
						id: "test_first_node_id",
						title: "This is a title that has not been updated"
					})-[c:Connects_TO {
						title: "This is an edge title that has not been updated"
					}]->(p:Reddit {
						id: "test_second_node_id",
						title: "This is a second title that has not been updated"
					})
				*/
				// Constructing the raw cypher string for node:
				var formattedLabels string = strings.Join(edge.Labels[:], ":")
				var edgeCypherQueryBuilder strings.Builder

				edgeCypherQueryBuilder.WriteString(fmt.Sprintf("MATCH (n {id: $from_id})-[e:%s {\n", formattedLabels))

				renamedMatchProperties := make(map[string]any)
				for propertyName := range edge.MatchProperties {
					renamedMatchProperties[propertyName] = fmt.Sprintf("match_%s", propertyName)
				}
				numMatchProperties := len(edge.MatchProperties)
				count := 1

				for matchPropertyName, prefixedPropertyName := range renamedMatchProperties {

					switch matchPropertyName {
					default:
						if count == numMatchProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s \n", matchPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: $%s, \n", matchPropertyName, prefixedPropertyName))
						}
					case "date":
						if count == numMatchProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", matchPropertyName, prefixedPropertyName))
						}
					case "datetime":
						if count == numMatchProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: datetime($%s), \n", matchPropertyName, prefixedPropertyName))
						}
					case "published_date":
						if count == numMatchProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s) \n", matchPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("\t %s: date($%s), \n", matchPropertyName, prefixedPropertyName))
						}
					}
					count += 1
				}

				edgeCypherQueryBuilder.WriteString("}]->(m {id: $to_id}) \n SET ")

				remappedSetProperties := make(map[string]any)
				for setPropertyName := range edge.SetProperties {
					remappedSetProperties[setPropertyName] = fmt.Sprintf("set_%s", setPropertyName)
				}

				numSetProperties := len(edge.SetProperties)
				count = 1

				for setPropertyName, prefixedPropertyName := range remappedSetProperties {

					switch setPropertyName {
					default:
						if count == numSetProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = $%s \n", setPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = $%s, \n", setPropertyName, prefixedPropertyName))
						}
					case "date":
						if count == numSetProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = date($%s) \n", setPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = date($%s), \n", setPropertyName, prefixedPropertyName))
						}
					case "published_date":
						if count == numSetProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = date($%s) \n", setPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = date($%s), \n", setPropertyName, prefixedPropertyName))
						}
					case "datetime":
						if count == numSetProperties {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = datetime($%s) \n", setPropertyName, prefixedPropertyName))
						} else {
							edgeCypherQueryBuilder.WriteString(fmt.Sprintf("e.%s = datetime($%s), \n", setPropertyName, prefixedPropertyName))
						}
					}
					count += 1
				}

				edgeCypherQueryBuilder.WriteString("RETURN e")

				// Merging all of the relevant properties into a single map to pass to tx.Run:
				totalPropertyMap := make(map[string]any)
				for matchPropertyName, prefixedPropertyName := range renamedMatchProperties {
					totalPropertyMap[prefixedPropertyName.(string)] = edge.MatchProperties[matchPropertyName]
				}
				for setPropertyName, prefixedPropertyName := range remappedSetProperties {
					totalPropertyMap[prefixedPropertyName.(string)] = edge.SetProperties[setPropertyName]
				}
				totalPropertyMap["from_id"] = edge.Connection.From
				totalPropertyMap["to_id"] = edge.Connection.To

				edgeQueryResponse, err := tx.Run(ctx, edgeCypherQueryBuilder.String(), totalPropertyMap)
				if err != nil {
					return nil, err
				}

				if edgeQueryResponse.Next(ctx) {
					fullQueryResult = append(fullQueryResult, edgeQueryResponse.Record().AsMap())
					log.Printf("Updated Edge connection")
				}

			}
		}

		return fullQueryResult, nil

	})

	if err != nil {
		return nil, err
	}
	return graphResultMap.([]map[string]any), nil

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
