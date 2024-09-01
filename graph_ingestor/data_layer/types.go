package datalayer

import "time"

type Neo4JEnvironment struct {
	URI      string
	User     string
	Password string
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
