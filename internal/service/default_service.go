package service

import (
	"go-core/core/pkg/security"
	"go-core/internal/model"
	"go-core/internal/repository"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pquerna/otp/totp"
	"go.opentelemetry.io/otel"
	"golang.org/x/crypto/bcrypt"
)

type TokenEncryptor interface {
	Encrypt(plaintext string) (string, error)
	Decrypt(cryptoText string) (string, error)
}

type DefaultService struct {
	repo       *repository.DefaultRepository
	encryptor  TokenEncryptor
	jwtManager *security.JWTManager
	isProd     bool
}

func NewDefaultService(repo *repository.DefaultRepository, enc TokenEncryptor, jwtManager *security.JWTManager, isProd bool) *DefaultService {
	return &DefaultService{
		repo:       repo,
		encryptor:  enc,
		jwtManager: jwtManager,
		isProd:     isProd,
	}
}

var tracer = otel.Tracer("default-service")

func (r *DefaultService) DefaultFunc() error {
	return nil
}