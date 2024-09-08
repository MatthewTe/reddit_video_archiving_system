package reddit

import (
	"context"
	"testing"
	"time"

	"github.com/MatthewTe/reddit_video_archiving_system/graph_ingestor/data_layer/config"
	"github.com/stretchr/testify/assert"
)

var ctx context.Context = context.Background()
var env config.Neo4JEnvironment = config.Neo4JEnvironment{
	URI:      "neo4j://localhost:7687",
	User:     "neo4j",
	Password: "test_password",
}

// Input Values
var exampleRedditPost RedditPost = RedditPost{
	Id:               "example_reddit_id",
	Subreddit:        "example_subreddit",
	Url:              "https://example_url.com",
	Title:            "Example Reddit Title",
	StaticDownloaded: true,
	Screenshot:       "example_reddit_id/screenshot.png",
	JsonPost:         "example_reddit_id/post.json",
	CreatedDate:      time.Date(2021, 8, 15, 14, 30, 45, 100, time.Local),
	StaticRootUrl:    "example_reddit_id/",
	StaticFileType:   "hosted:video",
}
var exampleRedditUser RedditUser = RedditUser{
	AuthorName:     "test_authorname",
	AuthorFullName: "t2_3ijf60sy",
}
var exampleSecondRedditUser RedditUser = RedditUser{
	AuthorName:     "test_second_authorname",
	AuthorFullName: "t2_3ijf6011",
}
var exampleRedditComments []RedditComment = []RedditComment{
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md5",
		Body:            "This is an example of a body",
		AssociatedUser:  exampleRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md6",
		Body:            "This is an example of a body part 2",
		AssociatedUser:  exampleRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md7",
		Body:            "This is an example of a body from a different user",
		AssociatedUser:  exampleSecondRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
}

// Test Structs to confirm outputs from methods:
var TestRawRedditOutput RawRedditPostResult = RawRedditPostResult{
	Id:               "example_reddit_id",
	Subreddit:        "example_subreddit",
	Url:              "https://example_url.com",
	Title:            "Example Reddit Title",
	StaticDownloaded: false,
	CreatedDate:      time.Date(2021, 8, 15, 14, 30, 45, 100, time.Local),
	StaticRootUrl:    "example_reddit_id/",
	StaticFileType:   "hosted:video",
}
var TestRedditUserOutputs RedditUser = RedditUser{
	AuthorName:     "test_authorname",
	AuthorFullName: "t2_3ijf60sy",
}
var TestRedditComments []RedditComment = []RedditComment{
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md5",
		Body:            "This is an example of a body",
		AssociatedUser:  exampleRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md6",
		Body:            "This is an example of a body part 2",
		AssociatedUser:  exampleRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
	{
		RedditPostId:    "example_reddit_id",
		CommentId:       "l7o9md7",
		Body:            "This is an example of a body from a different user",
		AssociatedUser:  exampleSecondRedditUser,
		PostedTimestamp: time.Date(2021, 8, 15, 14, 30, 45, 0, time.UTC),
	},
}

func TestInsertRedditPost(t *testing.T) {

	databaseCleared, err := config.FullyDeleteDatabase(env, ctx)
	if !databaseCleared {
		t.Error(err)
	}

	CreatedRawRedditPost, err := InsertRedditPost(exampleRedditPost, env, ctx)
	if err != nil {
		t.Error(err)
	}

	if !assert.EqualExportedValues(t, CreatedRawRedditPost, TestRawRedditOutput) {
		t.Errorf("output from function: %v \n test: %v", CreatedRawRedditPost, TestRawRedditOutput)
	}
}

func TestAttachRedditPostStaticFilesHostedVideo(t *testing.T) {

	databaseCleared, err := config.FullyDeleteDatabase(env, ctx)
	if !databaseCleared {
		t.Error(err)
	}

	_, err = InsertRedditPost(exampleRedditPost, env, ctx)
	if err != nil {
		t.Error(err)
	}

	CreatedAttachedStaticFilesHostedVideo, err := AttachRedditPostStaticFiles(exampleRedditPost, env, ctx)
	if err != nil {
		t.Error(err)
	}

	var ComparingHostedVideoStaticFileOutput RedditPostStaticFileResult = RedditPostStaticFileResult{
		RedditPostId:          "example_reddit_id",
		StaticRootUrl:         "example_reddit_id/",
		StaticFileType:        "hosted:video",
		SpecificStaticFileUrl: "example_reddit_id/hosted_video.mpd",
		StaticDownloaded:      true,
		Screenshot:            "example_reddit_id/screenshot.png",
		JsonPost:              "example_reddit_id/post.json",
	}

	if !assert.EqualExportedValues(t, CreatedAttachedStaticFilesHostedVideo, ComparingHostedVideoStaticFileOutput) {
		t.Error(err)
	}
}

func TestAttachRedditUser(t *testing.T) {

	databaseCleared, err := config.FullyDeleteDatabase(env, ctx)
	if !databaseCleared {
		t.Error(err)
	}

	createdRedditPost, err := InsertRedditPost(exampleRedditPost, env, ctx)
	if err != nil {
		t.Error(err)
	}

	CreatedAttachedRedditUser, err := AttachRedditUser(createdRedditPost, exampleRedditUser, env, ctx)
	if err != nil {
		t.Error(err)
	}

	var ComparingAttachedRedditUserOutput AttachedRedditUserResult = AttachedRedditUserResult{
		ParentPost:   TestRawRedditOutput,
		AttachedUser: TestRedditUserOutputs,
	}

	if !assert.EqualExportedValues(t, ComparingAttachedRedditUserOutput, CreatedAttachedRedditUser) {
		t.Error(err)
	}
}

func TestAppendRedditPostComments(t *testing.T) {
	databaseCleared, err := config.FullyDeleteDatabase(env, ctx)
	if !databaseCleared {
		t.Error(err)
	}

	createdRedditPost, err := InsertRedditPost(exampleRedditPost, env, ctx)
	if err != nil {
		t.Error(err)
	}

	existingRedditPostResult, redditCommentsResult, err := ApppendRedditPostComments(
		createdRedditPost, exampleRedditComments, env, ctx)

	if !assert.Equal(t, redditCommentsResult, TestRedditComments) {
		t.Error(err)
	}

	if !assert.EqualExportedValues(t, existingRedditPostResult, TestRawRedditOutput) {
		t.Error(err)
	}

}
