package v3transactionrepo

import (
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/transactionrepo/v3transactionrepo/v3transactiondberrors"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type V3TransactionDBRepo interface {
	CreateTransaction(tx *models.V3Transaction) error
}

type V3TransactionDBRepoDependencies struct {
	Database *pgdatabase.PgDatabase
}

func (d *V3TransactionDBRepoDependencies) validate() error {
	if d.Database == nil {
		return errors.New("token repo dependencies database cannot be nil")
	}

	return nil
}

type transactionDBRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func NewDBRepo(dependencies V3TransactionDBRepoDependencies) (V3TransactionDBRepo, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	return &transactionDBRepo{
		pgDatabase: dependencies.Database,
	}, nil
}

func (r *transactionDBRepo) CreateTransaction(tx *models.V3Transaction) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}

	query := psql.
		Insert(models.V3_TRANSACTION_TABLE).
		Columns(
			models.V3_TRANSACTION_CHAIN_ID,
			models.V3_TRANSACTION_POOL_ADDRESS,
			models.V3_TRANSACTION_TX_HASH,
			models.V3_TRANSACTION_TX_TIMESTAMP,
			models.V3_TRANSACTION_BLOCK_NUMBER,
			models.V3_TRANSACTION_AMOUNT0,
			models.V3_TRANSACTION_AMOUNT1,
			models.V3_TRANSACTION_ARCHIVE_TOKEN0_USD_PRICE,
			models.V3_TRANSACTION_ARCHIVE_TOKEN1_USD_PRICE,
		).Values(
		tx.ChainID,
		tx.PoolAddress,
		tx.TxHash,
		tx.TxTimestamp,
		tx.BlockNumber,
		tx.Amount0.String(),
		tx.Amount1.String(),
		tx.ArchiveToken0USDPrice.Text('f', -1),
		tx.ArchiveToken1USDPrice.Text('f', -1),
	)

	_, err = query.RunWith(db).Exec()
	if err != nil {
		fmt.Println(err)
		return v3transactiondberrors.ErrUnableToCreateTransaction
	}

	return nil
}
