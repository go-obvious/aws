package s3_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"

	s3api "github.com/go-obvious/aws/s3"
)

func TestIsNotFoundErr(t *testing.T) {
	is := awserr.New(s3.ErrCodeNoSuchKey, "totally not here, dude", nil)
	not1 := errors.New("not a not found error")
	not2 := awserr.New(s3.ErrCodeInvalidObjectState, "totally not valid, dude", nil)

	assert.True(t, s3api.IsNotFoundErr(is))
	assert.False(t, s3api.IsNotFoundErr(not1))
	assert.False(t, s3api.IsNotFoundErr(not2))
}
