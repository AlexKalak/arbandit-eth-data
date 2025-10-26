package v2pairsrepo

import (
	"fmt"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/periphery/pgdatabase"
)

type V2PairRepo interface {
	GetPairs() ([]models.UniswapV2Pair, error)
	GetPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error)
	GetNonDustyPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error)
	UpdatePairsIsDusty(pairs []models.UniswapV2Pair) error
	UpdatePairsAmount0Amount1BlockNumber(pairs []models.UniswapV2Pair) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type v2pairRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func New() (V2PairRepo, error) {
	pgDatabase, err := pgdatabase.New()
	if err != nil {
		return &v2pairRepo{}, err
	}

	return &v2pairRepo{
		pgDatabase: pgDatabase,
	}, nil
}

func (r *v2pairRepo) GetPairs() ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0,
			models.UNISWAP_V2_PAIR_TOKEN1,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
		).
		From(models.UNISWAP_V2_PAIR_TABLE)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var pairs = []models.UniswapV2Pair{}
	for rows.Next() {
		var pair models.UniswapV2Pair

		amount0Str := ""
		amount1Str := ""

		err := rows.Scan(
			&pair.Address,
			&pair.ChainID,
			&pair.Token0,
			&pair.Token1,
			&amount0Str,
			&amount1Str,
			&pair.FeeTier,
			&pair.IsDusty,
			&pair.BlockNumber,
		)

		if err != nil {
			return nil, err
		}

		amount0 := new(big.Int)
		_, ok := amount0.SetString(amount0Str, 10)
		if !ok {
			amount0 = big.NewInt(0)
		}
		amount1 := new(big.Int)
		_, ok = amount1.SetString(amount1Str, 10)
		if !ok {
			amount1 = big.NewInt(0)
		}

		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairRepo) GetPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0,
			models.UNISWAP_V2_PAIR_TOKEN1,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
		).
		From(models.UNISWAP_V2_PAIR_TABLE).
		Where(sq.Eq{models.UNISWAP_V2_PAIR_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var pairs = []models.UniswapV2Pair{}
	for rows.Next() {
		var pair models.UniswapV2Pair

		amount0Str := ""
		amount1Str := ""

		err := rows.Scan(
			&pair.Address,
			&pair.ChainID,
			&pair.Token0,
			&pair.Token1,
			&amount0Str,
			&amount1Str,
			&pair.FeeTier,
			&pair.IsDusty,
			&pair.BlockNumber,
		)

		if err != nil {
			return nil, err
		}

		amount0 := new(big.Int)
		_, ok := amount0.SetString(amount0Str, 10)
		if !ok {
			amount0 = big.NewInt(0)
		}
		amount1 := new(big.Int)
		_, ok = amount1.SetString(amount1Str, 10)
		if !ok {
			amount1 = big.NewInt(0)
		}

		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairRepo) GetNonDustyPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0,
			models.UNISWAP_V2_PAIR_TOKEN1,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,
		).
		From(models.UNISWAP_V2_PAIR_TABLE).
		Where(sq.Eq{
			models.UNISWAP_V2_PAIR_CHAINID:  chainID,
			models.UNISWAP_V2_PAIR_IS_DUSTY: false,
		})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var pairs = []models.UniswapV2Pair{}
	for rows.Next() {
		var pair models.UniswapV2Pair

		amount0Str := ""
		amount1Str := ""

		err := rows.Scan(
			&pair.Address,
			&pair.ChainID,
			&pair.Token0,
			&pair.Token1,
			&amount0Str,
			&amount1Str,
			&pair.FeeTier,
			&pair.IsDusty,
			&pair.BlockNumber,
		)

		if err != nil {
			fmt.Println("Unable to scan token: ", err)
			return nil, err
		}

		amount0 := new(big.Int)
		_, ok := amount0.SetString(amount0Str, 10)
		if !ok {
			amount0 = big.NewInt(0)
		}
		amount1 := new(big.Int)
		_, ok = amount1.SetString(amount1Str, 10)
		if !ok {
			amount1 = big.NewInt(0)
		}

		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairRepo) UpdatePairsIsDusty(pairs []models.UniswapV2Pair) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pair := range pairs {
		queryMap := map[string]any{
			models.UNISWAP_V2_PAIR_IS_DUSTY: pair.IsDusty,
		}

		query := psql.
			Update(
				models.UNISWAP_V2_PAIR_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.UNISWAP_V2_PAIR_ADDRESS: pair.Address, models.UNISWAP_V2_PAIR_CHAINID: pair.ChainID})

		_, err = query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *v2pairRepo) UpdatePairsAmount0Amount1BlockNumber(pairs []models.UniswapV2Pair) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pair := range pairs {
		queryMap := map[string]any{
			models.UNISWAP_V2_PAIR_AMOUNT0: pair.Amount0.String(),
			models.UNISWAP_V2_PAIR_AMOUNT1: pair.Amount1.String(),
			models.UNISWAP_V3_POOL_BLOCK_NUMBER: pair.BlockNumber,
		}

		query := psql.
			Update(
				models.UNISWAP_V2_PAIR_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.UNISWAP_V2_PAIR_ADDRESS: pair.Address, models.UNISWAP_V2_PAIR_CHAINID: pair.ChainID})

		_, err = query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
