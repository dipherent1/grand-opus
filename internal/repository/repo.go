package repository

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

type UserRepository interface {
	FindByID(ctx context.Context, id string) (*User, error)
	Insert(ctx context.Context, user *User) error
	// ... other methods
}

type User struct {
	ID   string `bson:"_id"`
	Name string `bson:"name"`
}

// MongoUserRepository implements UserRepository
type MongoUserRepository struct {
	collection *mongo.Collection
}

// NewMongoUserRepository is the "constructor" that injects the MongoDB collection
func NewMongoUserRepository(collection *mongo.Collection) *MongoUserRepository {
	return &MongoUserRepository{collection: collection}
}

func (r *MongoUserRepository) FindByID(ctx context.Context, id string) (*User, error) {
	// Implementation using r.collection
	// ...
	return nil, nil
}

func (r *MongoUserRepository) Insert(ctx context.Context, user *User) error {
	// Implementation using r.collection
	// ...
	return nil
}
