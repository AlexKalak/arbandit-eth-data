package v2pairsrepo

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
)

type V2PairDBRepo interface {
	DeletePairsByChain(chainID uint) error
	GetPairs() ([]models.UniswapV2Pair, error)
	GetPairByIdentificator(identificator models.V2PairIdentificator) (models.UniswapV2Pair, error)
	GetPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error)
	GetNonDustyPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error)
	UpdatePairsIsDusty(pairs []models.UniswapV2Pair) error
	UpdatePairsAmount0Amount1BlockNumber(pairs []models.UniswapV2Pair) error
	UpdatePairsColumns(pairs []models.UniswapV2Pair, columns []string) error
	UpdatePairColumns(pair models.UniswapV2Pair, columns []string) error
	SetAllPairsToDusty(chainID uint) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type v2pairDBRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

type V2PairDBRepoDependencies struct {
	Database *pgdatabase.PgDatabase
}

func (p *V2PairDBRepoDependencies) validate() error {
	if p.Database == nil {
		return errors.New("v2 pair repo database dependency cannot be nil")
	}

	return nil
}

func NewDBRepo(dependencies V2PairDBRepoDependencies) (V2PairDBRepo, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	return &v2pairDBRepo{
		pgDatabase: dependencies.Database,
	}, nil
}

func (r *v2pairDBRepo) DeletePairsByChain(chainID uint) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}

	query := psql.
		Delete(models.UNISWAP_V2_PAIR_TABLE).
		Where(sq.Eq{models.UNISWAP_V2_PAIR_CHAINID: chainID})

	_, err = query.
		RunWith(db).Exec()

	if err != nil {
		return err
	}

	return nil

}

func (r *v2pairDBRepo) GetPairs() ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
			models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,

			models.UNISWAP_V2_ZFO_10USD_RATE,
			models.UNISWAP_V2_NON_ZFO_10USD_RATE,
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

		zfo10USDRateStr := ""
		nonZfo10USDRateStr := ""

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

			&zfo10USDRateStr,
			&nonZfo10USDRateStr,
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

		zfo10USDRate := new(big.Float)
		_, ok = zfo10USDRate.SetString(zfo10USDRateStr)
		if !ok {
			zfo10USDRate = big.NewFloat(0)
		}
		nonZfo10USDRate := new(big.Float)
		_, ok = nonZfo10USDRate.SetString(nonZfo10USDRateStr)
		if !ok {
			nonZfo10USDRate = big.NewFloat(0)
		}

		pair.Zfo10USDRate = zfo10USDRate
		pair.NonZfo10USDRate = nonZfo10USDRate
		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairDBRepo) GetPairByIdentificator(identificator models.V2PairIdentificator) (models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return models.UniswapV2Pair{}, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
			models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,

			models.UNISWAP_V2_ZFO_10USD_RATE,
			models.UNISWAP_V2_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V2_PAIR_TABLE).
		Where(sq.Eq{models.UNISWAP_V2_PAIR_ADDRESS: identificator.Address, models.UNISWAP_V2_PAIR_CHAINID: identificator.ChainID})

	row := query.
		RunWith(db).
		QueryRow()

	var pair models.UniswapV2Pair

	amount0Str := ""
	amount1Str := ""

	zfo10USDRateStr := ""
	nonZfo10USDRateStr := ""
	err = row.Scan(
		&pair.Address,
		&pair.ChainID,
		&pair.Token0,
		&pair.Token1,
		&amount0Str,
		&amount1Str,
		&pair.FeeTier,
		&pair.IsDusty,
		&pair.BlockNumber,
		&zfo10USDRateStr,
		&nonZfo10USDRateStr,
	)

	if err != nil {
		return models.UniswapV2Pair{}, err
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

	zfo10USDRate := new(big.Float)
	_, ok = zfo10USDRate.SetString(zfo10USDRateStr)
	if !ok {
		zfo10USDRate = big.NewFloat(0)
	}
	nonZfo10USDRate := new(big.Float)
	_, ok = nonZfo10USDRate.SetString(nonZfo10USDRateStr)
	if !ok {
		nonZfo10USDRate = big.NewFloat(0)
	}

	pair.Zfo10USDRate = zfo10USDRate
	pair.NonZfo10USDRate = nonZfo10USDRate
	pair.Amount0 = amount0
	pair.Amount1 = amount1

	return pair, nil
}

func (r *v2pairDBRepo) GetPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
			models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,

			models.UNISWAP_V2_ZFO_10USD_RATE,
			models.UNISWAP_V2_NON_ZFO_10USD_RATE,
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

		zfo10USDRateStr := ""
		nonZfo10USDRateStr := ""
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
			&zfo10USDRateStr,
			&nonZfo10USDRateStr,
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

		zfo10USDRate := new(big.Float)
		_, ok = zfo10USDRate.SetString(zfo10USDRateStr)
		if !ok {
			zfo10USDRate = big.NewFloat(0)
		}
		nonZfo10USDRate := new(big.Float)
		_, ok = nonZfo10USDRate.SetString(nonZfo10USDRateStr)
		if !ok {
			nonZfo10USDRate = big.NewFloat(0)
		}

		pair.Zfo10USDRate = zfo10USDRate
		pair.NonZfo10USDRate = nonZfo10USDRate
		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairDBRepo) GetNonDustyPairsByChainID(chainID uint) ([]models.UniswapV2Pair, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V2_PAIR_ADDRESS,
			models.UNISWAP_V2_PAIR_CHAINID,
			models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS,
			models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS,
			models.UNISWAP_V2_PAIR_AMOUNT0,
			models.UNISWAP_V2_PAIR_AMOUNT1,
			models.UNISWAP_V2_PAIR_FEE_TIER,
			models.UNISWAP_V2_PAIR_IS_DUSTY,
			models.UNISWAP_V2_PAIR_BLOCK_NUMBER,

			models.UNISWAP_V2_ZFO_10USD_RATE,
			models.UNISWAP_V2_NON_ZFO_10USD_RATE,
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

		zfo10USDRateStr := ""
		nonZfo10USDRateStr := ""

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
			&zfo10USDRateStr,
			&nonZfo10USDRateStr,
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

		zfo10USDRate := new(big.Float)
		_, ok = zfo10USDRate.SetString(zfo10USDRateStr)
		if !ok {
			zfo10USDRate = big.NewFloat(0)
		}
		nonZfo10USDRate := new(big.Float)
		_, ok = nonZfo10USDRate.SetString(nonZfo10USDRateStr)
		if !ok {
			nonZfo10USDRate = big.NewFloat(0)
		}

		pair.Zfo10USDRate = zfo10USDRate
		pair.NonZfo10USDRate = nonZfo10USDRate

		pair.Amount0 = amount0
		pair.Amount1 = amount1

		pairs = append(pairs, pair)
	}

	return pairs, nil
}

func (r *v2pairDBRepo) UpdatePairsIsDusty(pairs []models.UniswapV2Pair) error {
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

func (r *v2pairDBRepo) UpdatePairsAmount0Amount1BlockNumber(pairs []models.UniswapV2Pair) error {
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
			models.UNISWAP_V2_PAIR_AMOUNT0:      pair.Amount0.String(),
			models.UNISWAP_V2_PAIR_AMOUNT1:      pair.Amount1.String(),
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

func (r *v2pairDBRepo) UpdatePairsColumns(pairs []models.UniswapV2Pair, columns []string) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pair := range pairs {
		err := r.UpdatePairColumnsWithinTx(tx, pair, columns)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *v2pairDBRepo) UpdatePairColumnsWithinTx(tx *sql.Tx, pair models.UniswapV2Pair, columns []string) error {
	colMap := make(map[string]any)
	for _, col := range columns {
		colMap[col] = new(any)
	}

	arrayToDelete := make([]string, 0)
	for _, column := range columns {
		if _, ok := colMap[column]; ok {
			continue
		}
		arrayToDelete = append(arrayToDelete, column)
	}

	queryMap := map[string]any{
		models.UNISWAP_V2_PAIR_ADDRESS:        pair.Address,
		models.UNISWAP_V2_PAIR_CHAINID:        pair.ChainID,
		models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS: pair.Token0,
		models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS: pair.Token1,
		models.UNISWAP_V2_PAIR_AMOUNT0:        pair.Amount0.String(),
		models.UNISWAP_V2_PAIR_AMOUNT1:        pair.Amount1.String(),
		models.UNISWAP_V2_PAIR_FEE_TIER:       pair.FeeTier,
		models.UNISWAP_V2_PAIR_IS_DUSTY:       pair.IsDusty,
		models.UNISWAP_V2_PAIR_BLOCK_NUMBER:   pair.BlockNumber,

		models.UNISWAP_V2_ZFO_10USD_RATE:     pair.Zfo10USDRate.Text('f', -1),
		models.UNISWAP_V2_NON_ZFO_10USD_RATE: pair.NonZfo10USDRate.Text('f', -1),
	}

	for _, colToDelete := range arrayToDelete {
		delete(queryMap, colToDelete)

	}

	query := psql.
		Update(
			models.UNISWAP_V2_PAIR_TABLE,
		).SetMap(queryMap).
		Where(sq.Eq{models.UNISWAP_V2_PAIR_ADDRESS: pair.Address, models.UNISWAP_V2_PAIR_CHAINID: pair.ChainID})

	_, err := query.RunWith(tx).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (r *v2pairDBRepo) UpdatePairColumns(pair models.UniswapV2Pair, columns []string) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	colMap := make(map[string]any)
	for _, col := range columns {
		colMap[col] = new(any)
	}

	arrayToDelete := make([]string, 0)
	for _, column := range columns {
		if _, ok := colMap[column]; ok {
			continue
		}
		arrayToDelete = append(arrayToDelete, column)
	}

	queryMap := map[string]any{
		models.UNISWAP_V2_PAIR_ADDRESS:        pair.Address,
		models.UNISWAP_V2_PAIR_CHAINID:        pair.ChainID,
		models.UNISWAP_V2_PAIR_TOKEN0_ADDRESS: pair.Token0,
		models.UNISWAP_V2_PAIR_TOKEN1_ADDRESS: pair.Token1,
		models.UNISWAP_V2_PAIR_AMOUNT0:        pair.Amount0.String(),
		models.UNISWAP_V2_PAIR_AMOUNT1:        pair.Amount1.String(),
		models.UNISWAP_V2_PAIR_FEE_TIER:       pair.FeeTier,
		models.UNISWAP_V2_PAIR_IS_DUSTY:       pair.IsDusty,
		models.UNISWAP_V2_PAIR_BLOCK_NUMBER:   pair.BlockNumber,

		models.UNISWAP_V2_ZFO_10USD_RATE:     pair.Zfo10USDRate.Text('f', -1),
		models.UNISWAP_V2_NON_ZFO_10USD_RATE: pair.NonZfo10USDRate.Text('f', -1),
	}

	for _, colToDelete := range arrayToDelete {
		delete(queryMap, colToDelete)

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

	return tx.Commit()

}

func (r *v2pairDBRepo) SetAllPairsToDusty(chainID uint) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	queryMap := map[string]any{
		models.UNISWAP_V2_PAIR_IS_DUSTY: true,
	}

	query := psql.
		Update(
			models.UNISWAP_V2_PAIR_TABLE,
		).SetMap(queryMap).
		Where(sq.Eq{models.UNISWAP_V2_PAIR_CHAINID: chainID})

	_, err = query.RunWith(tx).Exec()
	if err != nil {
		return err
	}

	return tx.Commit()
}
