package service

import (
	"context"
	"fmt"

	"github.com/guangyang/tidal/internal/aggregator"
	"github.com/guangyang/tidal/internal/cache"
)

type GiftService struct {
	aggregator      *aggregator.Aggregator
	idempotentCache *cache.IdempotentCacheInmem
	giftCache       *cache.GiftCache
	walletCache     *cache.WalletCache
}

func NewGiftService(
	agg *aggregator.Aggregator,
	ic *cache.IdempotentCacheInmem,
	gc *cache.GiftCache,
	wc *cache.WalletCache,
) *GiftService {
	return &GiftService{
		aggregator:      agg,
		idempotentCache: ic,
		giftCache:       gc,
		walletCache:     wc,
	}
}

type SendGiftResult int

const (
	SendOK SendGiftResult = iota
	SendDuplicate
	SendGiftNotFound
	SendInsufficientBalance
	SendServerError
)

func (s *GiftService) SendGift(ctx context.Context, userID, roomID, anchorID, giftID, comboSeq int64) (SendGiftResult, error) {
	// 1. idempotent check
	if err := s.idempotentCache.SETNX(comboKey(userID, comboSeq)); err != nil {
		return SendDuplicate, nil
	}

	// 2. gift price lookup
	price, err := s.giftCache.GetPrice(ctx, giftID)
	if err != nil {
		return SendGiftNotFound, fmt.Errorf("gift %d: %w", giftID, err)
	}

	// 3. sliding-window aggregator
	key := aggregator.GenComboKey(userID, anchorID, giftID, roomID)
	result, _ := s.aggregator.Add(key, price)

	// safety valve: aggregator overloaded, client can retry
	if result == aggregator.WindowDropped {
		return SendServerError, nil
	}

	// 4. balance pre-deduct on every hit
	if err := s.walletCache.PreDeduct(ctx, userID, price); err != nil {
		if result == aggregator.WindowCreated {
			s.aggregator.Remove(key)
		}
		return SendInsufficientBalance, nil
	}

	return SendOK, nil
}

func comboKey(userID, comboSeq int64) string {
	return fmt.Sprintf("idempotent:%d:%d", userID, comboSeq)
}
