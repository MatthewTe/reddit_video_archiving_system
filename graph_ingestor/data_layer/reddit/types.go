package reddit

import "time"

type RedditPost struct {
	Id               string    `json:"id"`
	Subreddit        string    `json:"subreddit"`
	Url              string    `json:"url"`
	Title            string    `json:"title"`
	StaticDownloaded bool      `json:"static_downloaded_flag"`
	Screenshot       string    `json:"screenshot_path"`
	JsonPost         string    `json:"json_path"`
	CreatedDate      time.Time `json:"created_date"`
	StaticRootUrl    string    `json:"static_root_url"`
	StaticFileType   string    `json:"static_file_type"`
}

type RedditUser struct {
	AuthorName     string
	AuthorFullName string
}

type RedditComment struct {
	RedditPostId    string
	CommentId       string
	Body            string
	AssociatedUser  RedditUser
	PostedTimestamp time.Time
}

// Result Wrapper objects
type RawRedditPostResult struct {
	Id               string    `json:"id"`
	Subreddit        string    `json:"subreddit"`
	Url              string    `json:"url"`
	Title            string    `json:"title"`
	StaticDownloaded bool      `json:"static_downloaded_flag"`
	CreatedDate      time.Time `json:"created_date"`
	StaticRootUrl    string    `json:"static_root_url"`
	StaticFileType   string    `json:"static_file_type"`
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
type RedditCommentCreatedResult struct {
	existingRedditPost RawRedditPostResult
	createdComments    []RedditComment
}
type AttachedRedditUserResult struct {
	ParentPost   RawRedditPostResult
	AttachedUser RedditUser
}
