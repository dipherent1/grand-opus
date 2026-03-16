package domain

import "time"

type Content struct {
	Id            string    `bson:"_id,omitempty" json:"_id,omitempty"`
	Domain        string    `bson:"domain" json:"domain"`
	URL           string    `bson:"url" json:"url"`
	Title         string    `bson:"title" json:"title"`
	Desc          string    `bson:"desc" json:"desc"`
	Author        string    `bson:"author" json:"author"`
	RawHtml       string    `bson:"raw_html" json:"raw_html"`
	Content       string    `bson:"content" json:"content"`
	Text          string    `bson:"Text" json:"text"`
	JsRender      bool      `bson:"js_render" json:"js_render"`
	DatePublished time.Time `bson:"date_published" json:"date_published"`
	CreatedAt     time.Time `bson:"created_at" json:"created_at"`
	UpdatedAt     time.Time `bson:"updated_at" json:"updated_at"`
}
