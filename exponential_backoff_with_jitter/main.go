package main

// Refer: https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html

import (
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"
)

const MaxAttempts = 3
const BaseBackOff = time.Second
const MaxBackoff = 20 * time.Second

type Fn[T any] func() (T, error)

func IsRetryable(err error) bool {
	return true
}

func RetryWithExponentialBackoff[T any](f Fn[T]) (T, error) {
	attempts := 0
	for {
		ret, err := f()
		if err == nil {
			return ret, nil
		}
		if !IsRetryable(err) {
			return ret, err
		}
		if attempts+1 >= MaxAttempts {
			return ret, err
		}
		jitter := time.Duration(rand.Int64N(int64(BaseBackOff * (1 << attempts))))
		backoff := min(jitter, MaxBackoff)
		time.Sleep(backoff)

		attempts++
	}
}

func main() {

	testFn := func() (int, error) {
		v := rand.IntN(10)
		if v < 3 {
			return v, nil
		}
		return 0, errors.New("random error")
	}

	successCount := 0
	successCountWithRetry := 0
	for i := 0; i < 10; i++ {
		_, err := testFn()
		if err == nil {
			successCount++
		}

		_, err = RetryWithExponentialBackoff(testFn)
		if err == nil {
			successCountWithRetry++
		}

	}

	slog.Info("done", "successCount", successCount, "successCountWithRetry", successCountWithRetry)
}
