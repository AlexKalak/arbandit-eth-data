package v2transactionrepo

import (
	"errors"
	"fmt"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v2transactionrepo/v2transactiondberrors"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type V2TransactionDBRepo interface {
	CreateV2Swap(tx *models.V2Swap) error
	GetV2SwapsByChainID(chainID uint) ([]models.V2Swap, error)
}

type V2TransactionDBRepoDependencies struct {
	Database *pgdatabase.PgDatabase
}

func (d *V2TransactionDBRepoDependencies) validate() error {
	if d.Database == nil {
		return errors.New("token repo dependencies database cannot be nil")
	}

	return nil
}

type transactionDBRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func NewDBRepo(dependencies V2TransactionDBRepoDependencies) (V2TransactionDBRepo, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	return &transactionDBRepo{
		pgDatabase: dependencies.Database,
	}, nil
}

func (r *transactionDBRepo) CreateV2Swap(swap *models.V2Swap) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}

	query := psql.
		Insert(models.V2_SWAP_TABLE).
		Columns(
			models.V2_SWAP_CHAIN_ID,
			models.V2_SWAP_PAIR_ADDRESS,
			models.V2_SWAP_TX_HASH,
			models.V2_SWAP_TX_TIMESTAMP,
			models.V2_SWAP_BLOCK_NUMBER,
			models.V2_SWAP_AMOUNT0,
			models.V2_SWAP_AMOUNT1,
			models.V2_SWAP_ARCHIVE_TOKEN0_USD_PRICE,
			models.V2_SWAP_ARCHIVE_TOKEN1_USD_PRICE,
		).Values(
		swap.ChainID,
		swap.PairAddress,
		swap.TxHash,
		swap.TxTimestamp,
		swap.BlockNumber,
		swap.Amount0.String(),
		swap.Amount1.String(),
		swap.ArchiveToken0USDPrice.Text('f', -1),
		swap.ArchiveToken1USDPrice.Text('f', -1),
	)

	_, err = query.RunWith(db).Exec()
	if err != nil {
		fmt.Println(err)
		return v2transactiondberrors.ErrUnableToCreateTransaction
	}

	return nil
}

func (r *transactionDBRepo) GetV2SwapsByChainID(chainID uint) ([]models.V2Swap, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.V2_SWAP_ID,
			models.V2_SWAP_CHAIN_ID,
			models.V2_SWAP_PAIR_ADDRESS,
			models.V2_SWAP_TX_HASH,
			models.V2_SWAP_TX_TIMESTAMP,
			models.V2_SWAP_BLOCK_NUMBER,
			models.V2_SWAP_AMOUNT0,
			models.V2_SWAP_AMOUNT1,
			models.V2_SWAP_ARCHIVE_TOKEN0_USD_PRICE,
			models.V2_SWAP_ARCHIVE_TOKEN1_USD_PRICE,
		).
		From(models.V2_SWAP_TABLE).
		Where(sq.Eq{models.V2_SWAP_CHAIN_ID: chainID}).OrderBy(models.V2_SWAP_ID)

	rows, err := query.RunWith(db).Query()
	if err != nil {
		return nil, err
	}

	res := []models.V2Swap{}
	for rows.Next() {
		var swap models.V2Swap

		amount0Str := ""
		amount1Str := ""
		archiveToken0USDPriceStr := ""
		archiveToken1USDPriceStr := ""

		err := rows.Scan(
			&swap.ID,
			&swap.ChainID,
			&swap.PairAddress,
			&swap.TxHash,
			&swap.TxTimestamp,
			&swap.BlockNumber,
			&amount0Str,
			&amount1Str,
			&archiveToken0USDPriceStr,
			&archiveToken1USDPriceStr,
		)
		if err != nil {
			continue
		}

		amount0, ok := new(big.Int).SetString(amount0Str, 10)
		if !ok {
			continue
		}
		amount1, ok := new(big.Int).SetString(amount1Str, 10)
		if !ok {
			continue
		}
		archiveToken0USDPrice, ok := new(big.Float).SetString(archiveToken0USDPriceStr)
		if !ok {
			continue
		}
		archiveToken1USDPrice, ok := new(big.Float).SetString(archiveToken1USDPriceStr)
		if !ok {
			continue
		}

		swap.Amount0 = amount0
		swap.Amount1 = amount1
		swap.ArchiveToken0USDPrice = archiveToken0USDPrice
		swap.ArchiveToken1USDPrice = archiveToken1USDPrice

		res = append(res, swap)
	}

	return res, nil
}
