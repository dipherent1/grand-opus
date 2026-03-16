package service

import (
	"context"

	"github.com/dipherent1/grand-opus/internal/repository"
)

// UserService handles business logic and depends on the UserRepository interface
type UserService struct {
	userRepo repository.UserRepository
}

// NewUserService injects the repository dependency
func NewUserService(userRepo repository.UserRepository) *UserService {
	return &UserService{userRepo: userRepo}
}

func (s *UserService) RegisterUser(ctx context.Context, id, name string) error {
	user := &repository.User{ID: id, Name: name}
	return s.userRepo.Insert(ctx, user) // Use the injected dependency
}
