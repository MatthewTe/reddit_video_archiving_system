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
// TODO: Change the reddit post insertion logic to attach a screenshot and json file
func InsertRedditPost(newRedditPost RedditPost, env config.Neo4JEnvironment, ctx context.Context) (RedditPost, error) {

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
					(screenshot:Reddit:Screenshot:StaticFile:Image {path: $screenshot_path}),
					(json:Reddit:Json:StaticFile {path: $json_path}),
					(post)-[:TAKEN {date: date($date)}]->(screenshot),
					(post)-[:EXTRACTED {date: date($date)}]->(json),
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
					post.static_root_url AS static_root_url,
					screenshot.screenshot_path AS screenshot_path,
					json.json_path AS json_path
				`,
				map[string]any{
					"subreddit_name":   newRedditPost.Subreddit,
					"published_date":   newRedditPost.CreatedDate.Format("2006-01-02"),
					"date":             newRedditPost.CreatedDate.Format("2006-01-02"),
					"static_file_type": newRedditPost.StaticFileType,
					"flag":             false,
					"id":               newRedditPost.Id,
					"url":              newRedditPost.Url,
					"title":            newRedditPost.Title,
					"static_root_url":  newRedditPost.StaticRootUrl,
					"screenshot_path":  newRedditPost.Screenshot,
					"json_path":        newRedditPost.JsonPost,
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
			createdRedditPost := RedditPost{
				Id:               redditPostResultMap["id"].(string),
				Subreddit:        redditPostResultMap["subreddit_name"].(string),
				Url:              redditPostResultMap["url"].(string),
				Title:            redditPostResultMap["title"].(string),
				StaticDownloaded: redditPostResultMap["static_downloaded"].(bool),
				CreatedDate:      createdDate,
				StaticRootUrl:    redditPostResultMap["static_root_url"].(string),
				StaticFileType:   redditPostResultMap["static_file_type"].(string),
				JsonPost:         redditPostResultMap["screenshot_path"].(string),
				Screenshot:       redditPostResultMap["json_path"].(string),
			}

			return createdRedditPost, nil
		})

	if err != nil {
		var emptyRedditPost RedditPost
		return emptyRedditPost, err
	}

	return redditPost.(RedditPost), err
}

func AttachRedditPostStaticFiles(existingRedditPost RedditPost, env config.Neo4JEnvironment, ctx context.Context) (RedditPostStaticFileResult, error) {

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
				`
				MATCH (post:Reddit:Post:Entity {id: $id})-[static_connection:STATIC_DOWNLOAD_STATUS]->(:StaticFile:Settings:StaticDownloadedFlag {downloaded: true})
				RETURN count(static_connection) > 0 AS already_processed
				`,
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
					(post:Reddit:Post:Entity {id: $id})-[conn_to_static_setting:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting_false:StaticFile:Settings:StaticDownloadedFlag {downloaded: false}),
					(post)-[:TAKEN]->(screenshot:Reddit:Screenshot:StaticFile:Image),
					(post)-[:EXTRACTED]->(json:Reddit:Json:StaticFile) 
				DELETE
					conn_to_static_setting
				RETURN
					post.id AS id, 
					screenshot.path AS screenshot_path,
					json.json_path AS json_path
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

func AddRedditUser(redditUser RedditUser, env config.Neo4JEnvironment, ctx context.Context) (RedditUser, error) {
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

	createdUser, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			createdRedditUserResult, err := tx.Run(
				ctx,
				`
				MERGE 
					(user:Reddit:User:Entity:Account { full_name: $author_full_name, name: $author_name })
				RETURN
					user.name AS authorName,
					user.full_name AS author_full_name
				`,
				map[string]any{
					"author_full_name": redditUser.AuthorFullName,
					"author_name":      redditUser.AuthorName,
				})
			if err != nil {
				return nil, err
			}

			createdRedditUserResponse, err := createdRedditUserResult.Single(ctx)
			if err != nil {
				return nil, err
			}
			createdRedditUserMap := createdRedditUserResponse.AsMap()

			var newRedditUser RedditUser = RedditUser{
				AuthorName:     createdRedditUserMap["author_name"].(string),
				AuthorFullName: createdRedditUserMap["author_full_name"].(string),
			}

			return newRedditUser, nil
		})

	if err != nil {
		var emptyRedditUser RedditUser
		return emptyRedditUser, err
	}

	return createdUser.(RedditUser), nil
}

func AttachRedditUser(existingRedditPost RedditPost, redditUser RedditUser, env config.Neo4JEnvironment, ctx context.Context) (AttachedRedditUserResult, error) {
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

	attachedRedditUserResult, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			redditPostExists, err := tx.Run(
				ctx,
				`
				MATCH (post:Reddit:Post:Entity {id: $id})
				RETURN count(post) > 0 AS reddit_post_exists
				`,
				map[string]any{
					"id": existingRedditPost.Id,
				})
			if err != nil {
				return nil, err
			}
			redditPostExistsResult, err := redditPostExists.Single(ctx)
			if err != nil {
				return nil, err
			}
			if !redditPostExistsResult.AsMap()["reddit_post_exists"].(bool) {
				return nil, fmt.Errorf("no post with id %s exists in db so cannot create and connect a reddit user. Exiting", existingRedditPost.Id)
			}

			createdRedditUserResponse, err := tx.Run(
				ctx,
				`
				MATCH 
					(post:Reddit:Post:Entity {id: $id})-[conn_to_static_setting:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag),
					(post)-[:TAKEN]->(screenshot:Reddit:Screenshot:StaticFile:Image),
					(post)-[:EXTRACTED]->(json:Reddit:Json:StaticFile),
					(post)-[:PUBLISHED_ON]->(published_date:Date),
					(post)-[:CONTAINS]->(static_file_type: StaticFileType:StaticFile:Settings),
					(post)-[:EXTRACTED_FORM]->(subreddit:Subreddit:Entity)
				MERGE
					(user:Reddit:User:Entity:Account 
						{
							name: $author_name,
							full_name: $author_full_name
						}
					)
				MERGE	
					(user)-[:POSTED {date: date($date)}]->(post)
				RETURN
					post.id AS post_id,
					subreddit.name AS post_subreddit,
					post.url AS post_url,
					post.title AS post_title,
					screenshot.screenshot_path AS post_screenshot_path,
					json.json_path AS post_json_path,
					static_downloaded_setting.downloaded AS post_static_downloaded,
					published_date.day AS post_created_date,
					post.static_root_url AS post_static_root_url,
					static_file_type.type AS post_static_file_type,
					user.name AS author_name,
					user.full_name AS author_full_name
				`,
				map[string]any{
					"id":               existingRedditPost.Id,
					"date":             existingRedditPost.CreatedDate.Format("2006-01-02"),
					"author_name":      redditUser.AuthorName,
					"author_full_name": redditUser.AuthorFullName,
				})
			if err != nil {
				return nil, err
			}

			createdRedditUserResult, err := createdRedditUserResponse.Single(ctx)
			if err != nil {
				return nil, err
			}
			createdUserResultMap := createdRedditUserResult.AsMap()

			fmt.Println(createdUserResultMap)

			createdDate := time.Time(createdUserResultMap["post_created_date"].(dbtype.Date))
			var attachedRedditPostResult RedditPost = RedditPost{
				Id:               createdUserResultMap["post_id"].(string),
				Subreddit:        createdUserResultMap["post_subreddit"].(string),
				Url:              createdUserResultMap["post_url"].(string),
				Title:            createdUserResultMap["post_title"].(string),
				StaticDownloaded: createdUserResultMap["post_static_downloaded"].(bool),
				CreatedDate:      createdDate,
				StaticRootUrl:    createdUserResultMap["post_static_root_url"].(string),
				StaticFileType:   createdUserResultMap["post_static_file_type"].(string),
				Screenshot:       createdUserResultMap["post_screenshot_path"].(string),
				JsonPost:         createdUserResultMap["post_json_path"].(string),
			}

			var createdRedditUser RedditUser = RedditUser{
				AuthorName:     createdUserResultMap["author_name"].(string),
				AuthorFullName: createdUserResultMap["author_full_name"].(string),
			}

			var attachedUserResult AttachedRedditUserResult = AttachedRedditUserResult{
				ParentPost:   attachedRedditPostResult,
				AttachedUser: createdRedditUser,
			}

			return attachedUserResult, nil
		})

	if err != nil {
		var emptyAttachedUserResult AttachedRedditUserResult = AttachedRedditUserResult{}
		return emptyAttachedUserResult, err
	}

	return attachedRedditUserResult.(AttachedRedditUserResult), err
}

func ApppendRedditPostComments(redditPost RedditPost, redditComments []RedditComment, env config.Neo4JEnvironment,
	ctx context.Context) (RedditPost, []RedditComment, error) {

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
	commentResponse, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			// Get the redditPost from the database. If not exists, exit not create.
			redditPostExistsResult, err := tx.Run(
				ctx,
				`
				MATCH (post:Reddit:Post:Entity {id: $id})
				RETURN count(post) > 0 AS post_exists
				`,
				map[string]any{
					"id": redditPost.Id,
				})
			if err != nil {
				return nil, err
			}
			redditPostExistsResponse, err := redditPostExistsResult.Single(ctx)
			if err != nil {
				return nil, err
			}
			postExists := redditPostExistsResponse.AsMap()["post_exists"].(bool)
			if !postExists {
				return nil, fmt.Errorf("reddit Post id: %s does not exist in the database. Unable to create and append posts", redditPost.Id)
			}

			rawRedditPostResult, err := tx.Run(
				ctx,
				`
				MATCH
					(post:Reddit:Post:Entity {id: $reddit_post_id}),
					(post)-[:TAKEN]->(screenshot:Reddit:Screenshot:StaticFile:Image),
					(post)-[:EXTRACTED]->(json:Reddit:Json:StaticFile),
					(post)-[static_connection:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag),
					(post)-[:PUBLISHED_ON]->(published_date:Date),
				 	(post)-[:CONTAINS]->(static_file_type: StaticFileType:StaticFile:Settings),
				 	(post)-[:EXTRACTED_FORM]->(subreddit:Subreddit:Entity)
				RETURN
					post.id AS post_id,
					subreddit.name AS post_subreddit,
					post.url AS post_url,
					post.title AS post_title,
					static_downloaded_setting.downloaded AS post_static_downloaded,
					date(published_date.day) AS post_created_date,
					post.static_root_url AS post_static_root_url,
					static_file_type.type AS post_static_file_type,
					screenshot.screenshot_path AS screenshot_path,
					json.json_path AS json_path
				`,
				map[string]any{
					"reddit_post_id": redditPost.Id,
				},
			)
			if err != nil {
				return nil, err
			}

			rawRedditPostResponse, err := rawRedditPostResult.Single(ctx)
			if err != nil {
				return nil, err
			}

			rawRedditPostResponseMap := rawRedditPostResponse.AsMap()
			// TODO: Why is this published date non-existent, what has happened???
			createdDate := time.Time(rawRedditPostResponseMap["post_created_date"].(dbtype.Date))
			fmt.Println(createdDate)
			var existingRedditPostResult RedditPost = RedditPost{
				Id:               rawRedditPostResponseMap["post_id"].(string),
				Subreddit:        rawRedditPostResponseMap["post_subreddit"].(string),
				Url:              rawRedditPostResponseMap["post_url"].(string),
				Title:            rawRedditPostResponseMap["post_title"].(string),
				StaticDownloaded: rawRedditPostResponseMap["post_static_downloaded"].(bool),
				CreatedDate:      createdDate,
				StaticRootUrl:    rawRedditPostResponseMap["post_static_root_url"].(string),
				StaticFileType:   rawRedditPostResponseMap["post_static_file_type"].(string),
				Screenshot:       rawRedditPostResponseMap["screenshot_path"].(string),
				JsonPost:         rawRedditPostResponseMap["json_path"].(string),
			}

			var createdRedditComments []RedditComment
			for _, comment := range redditComments {

				commentCreationResult, err := tx.Run(
					ctx,
					`
					MATCH
						(post:Reddit:Post:Entity {id: $reddit_post_id}),
						(post)-[:PUBLISHED_ON]->(published_date:Date),
						(post)-[:CONTAINS]->(static_file_type: StaticFileType:StaticFile:Settings),
						(post)-[:EXTRACTED_FORM]->(subreddit:Subreddit:Entity)
					MERGE (user:Reddit:User:Entity:Account {full_name: $author_full_name, name: $author_name})
					MERGE (date:Date {day: date($comment_posted_day)})
					CREATE
						(comment:Reddit:Entity:Comment {
							reddit_post_id: $reddit_post_id,
							id: $comment_id,
							body: $comment_body
						}),
						(comment)-[comment_user_conn:POSTED_BY {timestamp: datetime($full_posted_timestamp)}]->(user),
						(comment)-[:COMMENT_ON {timestamp: datetime($full_posted_timestamp)}]->(post),
						(comment)-[:POSTED_ON {timestamp: datetime($full_posted_timestamp)}]->(date)
					RETURN
						post.id AS reddit_post_id,
						user.full_name AS author_full_name,
						user.name AS author_name,
						comment.id AS comment_id,
						comment.body AS comment_body,
						date.day AS posted_day,
						comment_user_conn.timestamp AS comment_timestamp
					`,
					map[string]any{
						"reddit_post_id":        existingRedditPostResult.Id,
						"author_full_name":      comment.AssociatedUser.AuthorFullName,
						"author_name":           comment.AssociatedUser.AuthorName,
						"comment_posted_day":    comment.PostedTimestamp.Format("2006-01-02"),
						"comment_id":            comment.CommentId,
						"comment_body":          comment.Body,
						"full_posted_timestamp": comment.PostedTimestamp.Format("2006-01-02T15:04:05"),
					})
				if err != nil {
					return nil, err
				}

				commentCreationResponse, err := commentCreationResult.Single(ctx)
				if err != nil {
					return nil, err
				}
				commentCreationResponseMap := commentCreationResponse.AsMap()

				var createdCommentResponse RedditComment = RedditComment{
					RedditPostId: commentCreationResponseMap["reddit_post_id"].(string),
					CommentId:    commentCreationResponseMap["comment_id"].(string),
					Body:         commentCreationResponseMap["comment_body"].(string),
					AssociatedUser: RedditUser{
						AuthorName:     commentCreationResponseMap["author_name"].(string),
						AuthorFullName: commentCreationResponseMap["author_full_name"].(string),
					},
					PostedTimestamp: commentCreationResponseMap["comment_timestamp"].(time.Time).UTC(),
				}

				createdRedditComments = append(createdRedditComments, createdCommentResponse)
			}

			var createdRedditCommentFields RedditCommentCreatedResult = RedditCommentCreatedResult{
				existingRedditPost: existingRedditPostResult,
				createdComments:    createdRedditComments,
			}

			return createdRedditCommentFields, nil
		})

	if err != nil {
		var emptyRedditPost RedditPost = RedditPost{}
		var emptyRedditComments []RedditComment
		return emptyRedditPost, emptyRedditComments, err
	}

	result := commentResponse.(RedditCommentCreatedResult)

	return result.existingRedditPost, result.createdComments, nil
}

func CheckRedditPostExists(redditPostIds []string, env config.Neo4JEnvironment, ctx context.Context) ([]RedditPostsExistsResult, error) {

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
	postExistsResults, err := session.ExecuteWrite(ctx,
		func(tx neo4j.ManagedTransaction) (any, error) {

			var posts []RedditPostsExistsResult
			for _, postId := range redditPostIds {

				postExistsResult, err := tx.Run(
					ctx,
					`
					OPTIONAL MATCH (post:RedditPost {id: $id})-[:STATIC_DOWNLOAD_STATUS]->(static_downloaded_setting:StaticFile:Settings:StaticDownloadedFlag)
					RETURN 
						COALESCE(static_downloaded_setting.downloaded, false) AS static_downloaded_flag, 
						post IS NOT NULL AS post_exists
					`,
					map[string]any{"id": postId},
				)
				if err != nil {
					return nil, err
				}

				postExistsResponse, err := postExistsResult.Single(ctx)
				if err != nil {
					return nil, err
				}
				postExistsMap := postExistsResponse.AsMap()

				var PostExistsResult RedditPostsExistsResult = RedditPostsExistsResult{
					Id:               postId,
					Exists:           postExistsMap["post_exists"].(bool),
					StaticDownloaded: postExistsMap["static_downloaded_flag"].(bool),
				}

				posts = append(posts, PostExistsResult)
			}

			return posts, nil

		})

	if err != nil {
		var emptyPostExistsResult []RedditPostsExistsResult
		return emptyPostExistsResult, err
	}

	return postExistsResults.([]RedditPostsExistsResult), nil

}
