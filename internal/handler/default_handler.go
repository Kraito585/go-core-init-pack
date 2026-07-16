package handler

import (
	"go-core/core/pkg/response"
	"go-core/internal/model"
	"go-core/internal/service"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v3"
	"go.opentelemetry.io/otel"
)

type DefaultHandler struct {
	srv     *service.DefaultService
	is_prod bool
}

func NewDefaultHandler(srv *service.DefaultService, is_prod bool) *DefaultHandler {
	return &DefaultHandler{srv: srv}
}

var handlerTracer = otel.Tracer("http-handler")

func (r *DefaultHandler) DefaultFunc() error {
	return nil
}