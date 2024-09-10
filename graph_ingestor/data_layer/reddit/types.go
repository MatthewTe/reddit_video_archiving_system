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
	AuthorName     string `json:"author_name"`
	AuthorFullName string `json:"author_full_name"`
}

type RedditComment struct {
	RedditPostId    string
	CommentId       string
	Body            string
	AssociatedUser  RedditUser
	PostedTimestamp time.Time
}

// Result Wrapper objects
type RedditPostStaticFileResult struct {
	RedditPostId          string `json:"reddit_post_id"`
	StaticRootUrl         string `json:"static_root_url"`
	StaticFileType        string `json:"static_file_type"`
	SpecificStaticFileUrl string `json:"specific_static_file_url"`
	StaticDownloaded      bool   `json:"static_downloaded_flag"`
	Screenshot            string `json:"screenshot_path"`
	JsonPost              string `json:"json_path"`
}
type RedditCommentCreatedResult struct {
	existingRedditPost RedditPost
	createdComments    []RedditComment
}
type AttachedRedditUserResult struct {
	ParentPost   RedditPost `json:"parent_post"`
	AttachedUser RedditUser `json:"attached_user"`
}
type RedditPostsExistsResult struct {
	Id               string `json:"id"`
	Exists           bool   `json:"post_exists"`
	StaticDownloaded bool   `json:"static_downloaded_flag"`
}
