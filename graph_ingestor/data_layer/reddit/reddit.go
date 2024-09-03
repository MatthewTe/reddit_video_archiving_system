package reddit

import (
	"context"
	"fmt"
	"time"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
)

// Reddit Post
func InsertRedditPost(newRedditPost RedditPost, env config.Neo4JEnvironment, ctx context.Context) (RawRedditPost, error) {

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
			// 2) Check to see if the static downloaded node has been created. If not create it.
			// 3) Check to see if the date node (day) has been created. If not create it.
			// 4) Check to see if the static file type associated w/ the post exists. If not, create.
			_, err := tx.Run(
				ctx,
				`MERGE (subreddit:Subreddit:Entity {name: $subreddit_name})
				 MERGE (static_downloaded_setting_true:StaticFile:Settings:StaticDownloadedFlag {downloaded: true})
				 MERGE (static_downloaded_setting_false:StaticFile:Settings:StaticDownloadedFlag {downloaded: false})
				 MERGE (date:Date {day: date($day)})
				 MERGE (static_file_type:StaticFileType:StaticFile:Settings {type: $static_file_type})
				`,
				map[string]any{
					"subreddit_name":   newRedditPost.Subreddit,
					"flag":             false,
					"day":              newRedditPost.CreatedDate.Format("2006-01-02"),
					"static_file_type": newRedditPost.StaticFileType,
				})
			if err != nil {
				return nil, err
			}

			// -- At this point all the supporting nodes have been created, but no associations.
			// 5) Create the Reddit Post node w/ key field {id, url, static_root_url}
			record, err := tx.Run(
				ctx,
				`
				MATCH 
					(subreddit:Subreddit:Entity {name: $subreddit_name}),
					(published_date: Date {day: date($published_date)}),
					(static_file_type: StaticFileType:StaticFile:Settings {type: $static_file_type}),
					(static_downloaded_setting_false:StaticFile:Settings:StaticDownloadedFlag {downloaded: false})
				CREATE 
					(post:Reddit:Post:Entity 
						{
							id: $id, 
							url: $url, 
							title: $title, 
							static_root_url: $static_root_url,
							published_date: $published_date
						}
					),
					(post)-[:EXTRACTED_FORM]->(subreddit), 
					(post)-[:PUBLISHED_ON]->(published_date), 
					(post)-[:CONTAINS]->(static_file_type),
					(post)-[:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting_false)
				RETURN 
					post.id AS id,
					subreddit.name AS subreddit_name,
					post.url AS url,
					post.title AS title,
					static_downloaded_setting_false.downloaded AS static_downloaded,
					published_date.day AS created_date,
					static_file_type.type AS static_file_type,
					post.static_root_url AS static_root_url
				`,
				map[string]any{
					"subreddit_name":   newRedditPost.Subreddit,
					"published_date":   newRedditPost.CreatedDate.Format("2006-01-02"),
					"static_file_type": newRedditPost.StaticFileType,
					"flag":             false,
					"id":               newRedditPost.Id,
					"url":              newRedditPost.Url,
					"title":            newRedditPost.Title,
					"static_root_url":  newRedditPost.StaticRootUrl,
				})
			if err != nil {
				return nil, err
			}

			createdRedditPostResult, err := record.Single(ctx)
			if err != nil {
				return nil, err
			}

			redditPostResultMap := createdRedditPostResult.AsMap()
			createdDate := time.Time(redditPostResultMap["created_date"].(dbtype.Date))
			createdRedditPost := RawRedditPost{
				Id:               redditPostResultMap["id"].(string),
				Subreddit:        redditPostResultMap["subreddit_name"].(string),
				Url:              redditPostResultMap["url"].(string),
				Title:            redditPostResultMap["title"].(string),
				StaticDownloaded: redditPostResultMap["static_downloaded"].(bool),
				CreatedDate:      createdDate,
				StaticRootUrl:    redditPostResultMap["static_root_url"].(string),
				StaticFileType:   redditPostResultMap["static_file_type"].(string),
			}

			return createdRedditPost, nil
		})

	if err != nil {
		var emptyRawRedditPost RawRedditPost
		return emptyRawRedditPost, err
	}

	return redditPost.(RawRedditPost), err
}

func AppendRawRedditPostStaticFiles(existingRedditPost RedditPost, env config.Neo4JEnvironment, ctx context.Context) (RedditPostStaticFileResult, error) {

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

	session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	defer session.Close(ctx)

	createdRedditPostStaticResult, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			postAlreadyProcessedResult, err := tx.Run(
				ctx,
				"RETURN EXISTS((:Reddit:Post:Entity {id: $id})-[:STATIC_DOWNLOAD_STATUS]->(:StaticFile:Settings:StaticDownloadedFlag {downloaded: true})) AS already_processed",
				map[string]any{
					"id": existingRedditPost.Id,
				})
			if err != nil {
				return nil, err
			}
			postAlreadyProcessedResponse, err := postAlreadyProcessedResult.Single(ctx)
			if err != nil {
				return nil, err
			}
			var postAlreadyProcessed bool = postAlreadyProcessedResponse.AsMap()["already_processed"].(bool)
			if postAlreadyProcessed {
				return nil, fmt.Errorf("%s Post has already been processed. Skipping", existingRedditPost.Id)
			}

			// Create the nodes for the screenshot and the json file:
			createdBaseStaticFiles, err := tx.Run(
				ctx,
				`
				MATCH 
					(post:Reddit:Post:Entity {id: $id})-[conn_to_static_setting:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting_false:StaticFile:Settings:StaticDownloadedFlag {downloaded: false})
				CREATE 
					(screenshot:Reddit:Screenshot:StaticFile:Image {path: $screenshot_path}),
					(json:Reddit:Json:StaticFile {path: $json_path}),
					(post)-[:TAKEN {date: date($date)}]->(screenshot),
					(post)-[:EXTRACTED {date: date($date)}]->(json)
				DELETE
					conn_to_static_setting
				RETURN
					post.id AS id, 
					screenshot.path AS screenshot_path,
					json.path AS json_path
				`,
				map[string]any{
					"id":              existingRedditPost.Id,
					"screenshot_path": existingRedditPost.Screenshot,
					"json_path":       existingRedditPost.JsonPost,
					"date":            existingRedditPost.CreatedDate.Format("2006-01-02"),
				})

			if err != nil {
				return nil, err
			}

			existingPostResult, err := createdBaseStaticFiles.Single(ctx)
			if err != nil {
				return nil, err
			}

			existingPostMap := existingPostResult.AsMap()
			postId := existingPostMap["id"]

			// Connecting the static file type and setting the reddit post static settings to true:
			var staticFileConnectionResultsMap map[string]any

			// Hosted:Video
			if existingRedditPost.StaticFileType == "hosted:video" {

				rawStaticFileContentResult, err := tx.Run(
					ctx,
					`
					MATCH
						(post:Reddit:Post:Entity {id: $id}),
						(static_downloaded_setting_true:StaticFile:Settings:StaticDownloadedFlag {downloaded: true})
					CREATE 
						(video:Reddit:StaticFile:Video {path: $mpd_filepath}),
						(post)-[:EXTRACTED_ON {date: date($date)}]->(video),
						(post)-[:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting_true)
					RETURN 
						post.id AS post_id, 
						video.path AS static_file_path,
						static_downloaded_setting_true.downloaded AS static_file_downloaded
					`,
					map[string]any{
						"id":           postId,
						"mpd_filepath": fmt.Sprintf("%shosted_video.mpd", existingRedditPost.StaticRootUrl),
						"date":         existingRedditPost.CreatedDate.Format("2006-01-02"),
					})

				if err != nil {
					return nil, err
				}

				staticFileConnectionResponse, err := rawStaticFileContentResult.Collect(ctx)
				if err != nil {
					return nil, err
				}
				staticFileConnectionResultsMap = staticFileConnectionResponse[0].AsMap()

			} else {
				return nil, fmt.Errorf("%s static file type not supported", existingRedditPost.StaticFileType)
			}

			RedditPostStaticResult := RedditPostStaticFileResult{
				RedditPostId:          staticFileConnectionResultsMap["post_id"].(string),
				StaticRootUrl:         existingRedditPost.StaticRootUrl,
				StaticFileType:        existingRedditPost.StaticFileType,
				SpecificStaticFileUrl: staticFileConnectionResultsMap["static_file_path"].(string),
				StaticDownloaded:      staticFileConnectionResultsMap["static_file_downloaded"].(bool),
				Screenshot:            existingPostMap["screenshot_path"].(string),
				JsonPost:              existingPostMap["json_path"].(string),
			}

			return RedditPostStaticResult, nil
		})

	if err != nil {
		var emptyRawRedditPostStaticResult RedditPostStaticFileResult
		return emptyRawRedditPostStaticResult, err
	}

	return createdRedditPostStaticResult.(RedditPostStaticFileResult), nil

}

func InsertRawArticle() {}

func InsertSpatialIndexNode() {}
func InsertDatetimeNode()     {}

func InsertRawGoogleSearchResult() {}
