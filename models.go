package main

import (
	"time"

	"gopkg.in/mgutz/dat.v1"
)

type StockOrder struct {
	ID        int64        `db:"stock_order_id"`
	UserID    int64        `db:"user_id"`
	CreatedAt dat.NullTime `db:"created_at"`
}

type StockItemOrder struct {
	ID      int64 `db:"stock_item_order_id"`
	ItemID  int64 `db:"item_id"`
	OrderID int64 `db:"order_id"`
}

type StockInventoryTag struct {
	StockID int64  `db:"stock_id"`
	TypeID  int64  `db:"type_id"`
	Value   string `db:"value"`
}

type StockUserAnnotation struct {
	StockID    int64     `db:"stock_id"`
	CreatedBy  int64     `db:"created_user_id"`
	ModifiedBy int64     `db:"modified_user_id"`
	CreatedAt  time.Time `db:"created_at"`
	ModifiedAt time.Time `db:"updated_at"`
}

type PlasmidName struct {
	StockID int64  `db:"stock_id"`
	Name    string `db:"name"`
}

type User struct {
	ID        int64  `db:"auth_user_id"`
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
	Email     string `db:"email"`
	IsActive  bool   `db:"is_active"`
}

type UserInfo struct {
	ID            int64          `db:"auth_user_info_id"`
	UserID        int64          `db:"auth_user_id"`
	Organization  dat.NullString `db:"organization"`
	GroupName     dat.NullString `db:"group_name"`
	FirstAddress  dat.NullString `db:"first_address"`
	SecondAddress dat.NullString `db:"second_address"`
	City          dat.NullString `db:"city"`
	State         dat.NullString `db:"state"`
	Zipcode       dat.NullString `db:"zipcode"`
	Country       dat.NullString `db:"country"`
	Phone         dat.NullString `db:"phone"`
}

type UserRelationship struct {
	ID        int64 `db:"user_relationship_id"`
	IsActive  bool  `db:"is_active"`
	TypeId    int64 `db:"type_id"`
	SubjectId int64 `db:"subject_id"`
	ObjectId  int64 `db:"object_id"`
}

type Cv struct {
	CvId       int64          `db:"cv_id"`
	Name       string         `db:"name"`
	Definition dat.NullString `db:"definition"`
}

type Cvterm struct {
	CvtermId           int64          `db:"cvterm_id"`
	Name               string         `db:"name"`
	Definition         dat.NullString `db:"definition"`
	IsObsolete         int64          `db:"is_obsolete"`
	IsRelationshipType int64          `db:"is_relationshiptype"`
	CvId               int64          `db:"cv_id"`
}

type Dbxref struct {
	DbxrefId    int64          `db:"dbxref_id"`
	Accession   string         `db:"accession"`
	Version     dat.NullString `db:"version"`
	Description dat.NullString `db:"Description"`
	DbId        int64          `db:"db_id"`
}

type Db struct {
	DbId        int64          `db:db_id"`
	Name        string         `db:"name"`
	Description dat.NullString `db:"description"`
	Urlprefix   dat.NullString `db:"urlprefix"`
	Url         dat.NullString `db:"url"`
}
