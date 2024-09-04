package reddit

import "time"

type RawRedditPost struct {
	Id               string
	Subreddit        string
	Url              string
	Title            string
	StaticDownloaded bool
	CreatedDate      time.Time
	StaticRootUrl    string
	StaticFileType   string
}

type RedditPost struct {
	Id               string
	Subreddit        string
	Url              string
	Title            string
	StaticDownloaded bool
	Screenshot       string
	JsonPost         string
	CreatedDate      time.Time
	StaticRootUrl    string
	StaticFileType   string
}

type RedditPostStaticFileResult struct {
	RedditPostId          string
	StaticRootUrl         string
	StaticFileType        string
	SpecificStaticFileUrl string
	StaticDownloaded      bool
	Screenshot            string
	JsonPost              string
}

type RedditUser struct {
	AuthorName     string
	AuthorFullName string
}
type RedditComment struct {
	RedditPostId string
	CommentId    string
	Body         string
}
