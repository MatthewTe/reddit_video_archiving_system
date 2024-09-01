package datalayer

import (
	"context"
	"errors"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Reddit Post
func InsertRedditPost(newRedditPost RedditPost, env Neo4JEnvironment, ctx context.Context) {

	driver, err := neo4j.NewDriverWithContext(
		env.URI,
		neo4j.BasicAuth(env.User, env.Password, ""))
	if err != nil {
		panic(err)
	}

	defer driver.Close(ctx)

	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		panic(err)
	}

	// Not using the Session - just creating a unique constraint:
	_, err = neo4j.ExecuteQuery(ctx, driver,
		"CREATE CONSTRAINT post_id_unique IF NOT EXISTS FOR (post:Post) REQUIRE post.id IS UNIQUE",
		map[string]any{},
		neo4j.EagerResultTransformer,
		neo4j.ExecuteQueryWithDatabase("neo4j"))
	if err != nil {
		panic(err)
	}

	// Create session:
	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	// Creating a managed transaction that runs an ExecuteWrite() method.
	redditPost, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			// 1) Check to see if the subreddit node already exist, if not create it.
			_, err := tx.Run(
				ctx,
				"MERGE (subreddit:Subreddit:Entity {name: $subreddit_name})",
				map[string]any{
					"subreddit_name": newRedditPost.Subreddit,
				})
			if err != nil {
				return nil, err
			}

			// 2) Check to see if the static downloaded node has been created. If not create it.
			_, err = tx.Run(
				ctx,
				"MERGE (static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag {downloaded: $flag})",
				map[string]any{
					"flag": true,
				})
			if err != nil {
				return nil, err
			}

			// 3) Check to see if the date node (day) has been created. If not create it.
			_, err = tx.Run(
				ctx,
				"MERGE (date:Date {day: date($day)})",
				map[string]any{
					"day": newRedditPost.CreatedDate.Format("2006-01-02"),
				})
			if err != nil {
				return nil, err
			}

			// 4) Check to see if the static file type associated w/ the post exists. If not, create.
			_, err = tx.Run(
				ctx,
				"MERGE (static_file_type:StaticFileType:StaticFile:Settings {type: $static_file_type})",
				map[string]any{
					"static_file_type": newRedditPost.StaticFileType,
				})
			if err != nil {
				return nil, err
			}
			// -- At this point all the supporting nodes have been created, but no associations.

			// 5) Create the Reddit Post node w/ key field {id, url, static_root_url}
			redditPostCreationResult, err := tx.Run(
				ctx,
				`CREATE (p: Reddit:Post:Entity 
					{
						id: $id, 
						url: $url, 
						title: $title, 
						static_root_url: $static_root_url,
						published_date: $published_date
					}
				)
				RETURN p.id	AS id
				`,
				map[string]any{
					"id":              newRedditPost.Id,
					"url":             newRedditPost.Url,
					"static_root_url": newRedditPost.StaticRootUrl,
					"title":           newRedditPost.Title,
					"published_date":  newRedditPost.CreatedDate.Format("2006-01-02"),
				})
			if err != nil {
				return nil, err
			}

			postResult, err := redditPostCreationResult.Single(ctx)
			if err != nil {
				return nil, err
			}

			createdRedditPostId := postResult.AsMap()["id"]

			// 6) Connect the Reddit Post node to the subreddit node.
			// 7) Connect the Reddit Post node to the Date node
			// 8) Connect the Reddit Post node to the static file type node.
			PostNodeAssociationResults, err := tx.Run(
				ctx,
				`
				MATCH 
					(post: Reddit:Post:Entity {id: $id}), 
					(subreddit:Subreddit:Entity {name: $subreddit_name}),
					(published_date: Date {day: date($date_str)}),
					(static_file_type: StaticFileType:StaticFile:Settings {type: $static_file_type})
				CREATE 
					(post)-[:EXTRACTED_FORM]->(subreddit), 
					(post)-[:PUBLISHED_ON]->(published_date), 
					(post)-[:CONTAINS]->(static_file_type)
				RETURN post.id, subreddit.name, published_date.day, static_file_type.type
				`,
				map[string]any{
					"id":               createdRedditPostId,
					"subreddit_name":   newRedditPost.Subreddit,
					"date_str":         newRedditPost.CreatedDate.Format("2006-01-02"),
					"static_file_type": newRedditPost.StaticFileType,
				},
			)
			if err != nil {
				return nil, err
			}

			associationResult, err := PostNodeAssociationResults.Single(ctx)
			if err != nil {
				return nil, err
			}
			fmt.Println(associationResult.Values...)

			// 9) Create the static file nodes and connect it to the Reddit Post.
			// 		- Screenshot (StaticFile:Image:Screenshot)
			//		- Json (StaticFile:Json:RedditPost)
			_, err = tx.Run(
				ctx,
				`
				MATCH (post:Reddit:Post:Entity {id: $post_id})
				CREATE 
					(screenshot:Reddit:Screenshot:StaticFile:Image {path: $screenshot_path}),
					(json:Reddit:Json:StaticFile {path: $json_path}),
					(post)-[:TAKEN {date: date($date)}]->(screenshot),
					(post)-[:EXTRACTED {date: date($date)}]->(json)
				RETURN post.id, screenshot.path
				`,
				map[string]any{
					"post_id":         createdRedditPostId,
					"screenshot_path": newRedditPost.Screenshot,
					"json_path":       newRedditPost.JsonPost,
					"date":            newRedditPost.CreatedDate.Format("2006-01-02"),
				})
			if err != nil {
				return nil, err
			}
			// 		- Video (StaticFile:Video)
			if newRedditPost.StaticFileType == "hosted:video" {
				_, err = tx.Run(
					ctx,
					`
					MATCH (post:Reddit:Post:Entity {id: $post_id})
					CREATE 
						(video:Reddit:StaticFile:Video {path: $mpd_filepath}),
						(post)-[:EXTRACTED_ON {date: date($date)}]->(video)
					RETURN post.id, video.path
					`,
					map[string]any{
						"post_id":      createdRedditPostId,
						"mpd_filepath": fmt.Sprintf("%s/hosted_video.mpd", newRedditPost.StaticRootUrl),
						"date":         newRedditPost.CreatedDate.Format("2006-01-02"),
					})
				if err != nil {
					return nil, err
				}

			} else {
				return nil, errors.New("reddit Post static file type not supported")
			}

			// Connecting the static node flag set to true now that all static files have been ingested:
			_, err = tx.Run(
				ctx,
				`
				MATCH 
					(post:Reddit:Post:Entity {id: $post_id}),
					(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag {downloaded: $flag})
				CREATE
					(post)-[:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting)
				`,
				map[string]any{
					"post_id": createdRedditPostId,
					"flag":    true,
				})
			if err != nil {
				return nil, err
			}

			fullPostAssociationResult, err := tx.Run(
				ctx,
				`
				MATCH 
					(post:Reddit:Post:Entity {id: $post_id})-->(subreddit:Subreddit:Entity),
					(post)-->(published_date:Date),
					(post)-->(static_file_type:StaticFileType:StaticFile:Settings),
					(post)-->(screenshot:Reddit:Screenshot:StaticFile:Image),
					(post)-->(json:Reddit:Json:StaticFile),
					(post)-->(video:Reddit:StaticFile:Video),
    				(post)-->(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag)
				WHERE
					static_downloaded_setting = $flag
				RETURN post, subreddit, published_date, static_file_type, screenshot, json, video
				`,
				map[string]any{
					"post_id": createdRedditPostId,
					"flag":    true,
				})
			if err != nil {
				return nil, err
			}

			records, err := fullPostAssociationResult.Collect(ctx)
			if err != nil {
				return nil, err
			}

			return records, nil
		})

	if err != nil {
		panic(err)
	}
	fmt.Println(redditPost)
}
