package v3poolsrepo

import (
	"errors"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
)

type V3PoolDBRepo interface {
	GetPoolByPoolIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error)
	GetPools() ([]models.UniswapV3Pool, error)
	GetPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error)
	GetPoolsByChainIDOrdered(chainID uint) ([]models.UniswapV3Pool, error)
	GetPoolsByChainIDWith0BlockNumberOrdered(chainID uint) ([]models.UniswapV3Pool, error)
	GetNotDustyPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error)
	UpdatePoolsLiquiditySqrtPriceTickAndBlockNumber(pools []models.UniswapV3Pool) error
	UpdatePoolsIsDusty(pools []models.UniswapV3Pool) error
	SetAllPoolsToDusty(chainID uint) error
	UpdatePoolsLowerUpperNearTicks(pools []models.UniswapV3Pool) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type V3PoolDBRepoDependencies struct {
	Database *pgdatabase.PgDatabase
}

func (p *V3PoolDBRepoDependencies) validate() error {
	if p.Database == nil {
		return errors.New("v3 pool db repo database dependency cannot be nil")
	}

	return nil
}

type v3poolDBRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func NewDBRepo(dependenices V3PoolDBRepoDependencies) (V3PoolDBRepo, error) {
	if err := dependenices.validate(); err != nil {
		return nil, err
	}

	return &v3poolDBRepo{
		pgDatabase: dependenices.Database,
	}, nil
}

func (r *v3poolDBRepo) GetPoolByPoolIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: poolIdentificator.Address, models.UNISWAP_V3_POOL_CHAINID: poolIdentificator.ChainID})

	var pool models.UniswapV3Pool

	sqrtPriceX96Str := ""
	liquidityStr := ""

	zfo10str := ""
	nonzfo10str := ""

	err = query.
		RunWith(db).
		QueryRow().
		Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)

	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	sqrtPriceX96 := new(big.Int)
	_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
	if !ok {
		sqrtPriceX96 = big.NewInt(0)
	}
	liquidity := new(big.Int)
	_, ok = liquidity.SetString(liquidityStr, 10)
	if !ok {
		liquidity = big.NewInt(0)
	}

	zfo10 := new(big.Float)
	_, ok = zfo10.SetString(zfo10str)
	if !ok {
		zfo10 = big.NewFloat(0)
	}

	nonzfo10 := new(big.Float)
	_, ok = nonzfo10.SetString(nonzfo10str)
	if !ok {
		nonzfo10 = big.NewFloat(0)
	}

	pool.Zfo10USDRate = zfo10
	pool.NonZfo10USDRate = nonzfo10

	pool.SqrtPriceX96 = sqrtPriceX96
	pool.Liquidity = liquidity

	if err != nil {
		return models.UniswapV3Pool{}, err
	}

	return pool, nil
}

func (r *v3poolDBRepo) GetPools() ([]models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	pools := []models.UniswapV3Pool{}
	for rows.Next() {
		var pool models.UniswapV3Pool

		sqrtPriceX96Str := ""
		liquidityStr := ""

		zfo10str := ""
		nonzfo10str := ""

		err := rows.Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)
		if err != nil {
			return nil, err
		}

		sqrtPriceX96 := new(big.Int)
		_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
		if !ok {
			sqrtPriceX96 = big.NewInt(0)
		}
		liquidity := new(big.Int)
		_, ok = liquidity.SetString(liquidityStr, 10)
		if !ok {
			liquidity = big.NewInt(0)
		}

		zfo10 := new(big.Float)
		_, ok = zfo10.SetString(zfo10str)
		if !ok {
			zfo10 = big.NewFloat(0)
		}
		nonzfo10 := new(big.Float)
		_, ok = nonzfo10.SetString(nonzfo10str)
		if !ok {
			nonzfo10 = big.NewFloat(0)
		}

		pool.Zfo10USDRate = zfo10
		pool.NonZfo10USDRate = nonzfo10

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) GetPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	pools := []models.UniswapV3Pool{}
	for rows.Next() {
		var pool models.UniswapV3Pool

		sqrtPriceX96Str := ""
		liquidityStr := ""

		zfo10str := ""
		nonzfo10str := ""

		err := rows.Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)
		if err != nil {
			return nil, err
		}

		sqrtPriceX96 := new(big.Int)
		_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
		if !ok {
			sqrtPriceX96 = big.NewInt(0)
		}
		liquidity := new(big.Int)
		_, ok = liquidity.SetString(liquidityStr, 10)
		if !ok {
			liquidity = big.NewInt(0)
		}

		zfo10 := new(big.Float)
		_, ok = zfo10.SetString(zfo10str)
		if !ok {
			zfo10 = big.NewFloat(0)
		}
		nonzfo10 := new(big.Float)
		_, ok = nonzfo10.SetString(nonzfo10str)
		if !ok {
			nonzfo10 = big.NewFloat(0)
		}

		pool.Zfo10USDRate = zfo10
		pool.NonZfo10USDRate = nonzfo10

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) GetPoolsByChainIDOrdered(chainID uint) ([]models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_CHAINID: chainID}).
		OrderBy(models.UNISWAP_V3_POOL_ADDRESS)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	pools := []models.UniswapV3Pool{}
	for rows.Next() {
		var pool models.UniswapV3Pool

		sqrtPriceX96Str := ""
		liquidityStr := ""

		zfo10str := ""
		nonzfo10str := ""

		err := rows.Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)
		if err != nil {
			return nil, err
		}

		sqrtPriceX96 := new(big.Int)
		_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
		if !ok {
			sqrtPriceX96 = big.NewInt(0)
		}
		liquidity := new(big.Int)
		_, ok = liquidity.SetString(liquidityStr, 10)
		if !ok {
			liquidity = big.NewInt(0)
		}

		zfo10 := new(big.Float)
		_, ok = zfo10.SetString(zfo10str)
		if !ok {
			zfo10 = big.NewFloat(0)
		}
		nonzfo10 := new(big.Float)
		_, ok = nonzfo10.SetString(nonzfo10str)
		if !ok {
			nonzfo10 = big.NewFloat(0)
		}

		pool.Zfo10USDRate = zfo10
		pool.NonZfo10USDRate = nonzfo10

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) GetPoolsByChainIDWith0BlockNumberOrdered(chainID uint) ([]models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_CHAINID: chainID, models.UNISWAP_V3_POOL_BLOCK_NUMBER: 0}).
		OrderBy(models.UNISWAP_V3_POOL_ADDRESS)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	pools := []models.UniswapV3Pool{}
	for rows.Next() {
		var pool models.UniswapV3Pool

		sqrtPriceX96Str := ""
		liquidityStr := ""

		zfo10str := ""
		nonzfo10str := ""
		err := rows.Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)
		if err != nil {
			return nil, err
		}

		sqrtPriceX96 := new(big.Int)
		_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
		if !ok {
			sqrtPriceX96 = big.NewInt(0)
		}
		liquidity := new(big.Int)
		_, ok = liquidity.SetString(liquidityStr, 10)
		if !ok {
			liquidity = big.NewInt(0)
		}

		zfo10 := new(big.Float)
		_, ok = zfo10.SetString(zfo10str)
		if !ok {
			zfo10 = big.NewFloat(0)
		}
		nonzfo10 := new(big.Float)
		_, ok = nonzfo10.SetString(nonzfo10str)
		if !ok {
			nonzfo10 = big.NewFloat(0)
		}

		pool.Zfo10USDRate = zfo10
		pool.NonZfo10USDRate = nonzfo10

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) GetNotDustyPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.UNISWAP_V3_POOL_ADDRESS,
			models.UNISWAP_V3_POOL_CHAINID,
			models.UNISWAP_V3_POOL_TOKEN0,
			models.UNISWAP_V3_POOL_TOKEN1,
			models.UNISWAP_V3_POOL_SQRTPRICEX96,
			models.UNISWAP_V3_POOL_LIQUIDITY,

			models.UNISWAP_V3_POOL_TICK,
			models.UNISWAP_V3_POOL_TICK_SPACING,
			models.UNISWAP_V3_POOL_TICK_LOWER,
			models.UNISWAP_V3_POOL_TICK_UPPER,
			models.UNISWAP_V3_POOL_NEAR_TICKS,

			models.UNISWAP_V3_POOL_FEE_TIER,
			models.UNISWAP_V3_POOL_IS_DUSTY,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER,

			models.UNISWAP_V3_ZFO_10USD_RATE,
			models.UNISWAP_V3_NON_ZFO_10USD_RATE,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_CHAINID: chainID, models.UNISWAP_V3_POOL_IS_DUSTY: false})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	pools := []models.UniswapV3Pool{}
	for rows.Next() {
		var pool models.UniswapV3Pool

		sqrtPriceX96Str := ""
		liquidityStr := ""

		zfo10str := ""
		nonzfo10str := ""

		err := rows.Scan(
			&pool.Address,
			&pool.ChainID,
			&pool.Token0,
			&pool.Token1,
			&sqrtPriceX96Str,
			&liquidityStr,
			&pool.Tick,
			&pool.TickSpacing,
			&pool.TickLower,
			&pool.TickUpper,
			&pool.NearTicksJSON,
			&pool.FeeTier,
			&pool.IsDusty,
			&pool.BlockNumber,
			&zfo10str,
			&nonzfo10str,
		)
		if err != nil {
			return nil, err
		}

		sqrtPriceX96 := new(big.Int)
		_, ok := sqrtPriceX96.SetString(sqrtPriceX96Str, 10)
		if !ok {
			sqrtPriceX96 = big.NewInt(0)
		}
		liquidity := new(big.Int)
		_, ok = liquidity.SetString(liquidityStr, 10)
		if !ok {
			liquidity = big.NewInt(0)
		}

		zfo10 := new(big.Float)
		_, ok = zfo10.SetString(zfo10str)
		if !ok {
			zfo10 = big.NewFloat(0)
		}
		nonzfo10 := new(big.Float)
		_, ok = nonzfo10.SetString(nonzfo10str)
		if !ok {
			nonzfo10 = big.NewFloat(0)
		}

		pool.Zfo10USDRate = zfo10
		pool.NonZfo10USDRate = nonzfo10

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) UpdatePoolsLiquiditySqrtPriceTickAndBlockNumber(pools []models.UniswapV3Pool) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pool := range pools {
		queryMap := map[string]any{
			models.UNISWAP_V3_POOL_LIQUIDITY:    pool.Liquidity.String(),
			models.UNISWAP_V3_POOL_SQRTPRICEX96: pool.SqrtPriceX96.String(),
			models.UNISWAP_V3_POOL_TICK:         pool.Tick,
			models.UNISWAP_V3_POOL_TICK_SPACING: pool.TickSpacing,
			models.UNISWAP_V3_POOL_BLOCK_NUMBER: pool.BlockNumber,
		}

		query := psql.
			Update(
				models.UNISWAP_V3_POOL_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: pool.Address, models.UNISWAP_V3_POOL_CHAINID: pool.ChainID})

		_, err = query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *v3poolDBRepo) UpdatePoolsIsDusty(pools []models.UniswapV3Pool) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pool := range pools {
		queryMap := map[string]any{
			models.UNISWAP_V3_POOL_IS_DUSTY: pool.IsDusty,

			models.UNISWAP_V3_ZFO_10USD_RATE:     pool.Zfo10USDRate.Text('f', -1),
			models.UNISWAP_V3_NON_ZFO_10USD_RATE: pool.NonZfo10USDRate.Text('f', -1),
		}

		query := psql.
			Update(
				models.UNISWAP_V3_POOL_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: pool.Address, models.UNISWAP_V3_POOL_CHAINID: pool.ChainID})

		_, err = query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *v3poolDBRepo) SetAllPoolsToDusty(chainID uint) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	queryMap := map[string]any{
		models.UNISWAP_V3_POOL_IS_DUSTY: true,
	}

	query := psql.
		Update(
			models.UNISWAP_V3_POOL_TABLE,
		).SetMap(queryMap).
		Where(sq.Eq{models.UNISWAP_V3_POOL_CHAINID: chainID})

	_, err = query.RunWith(tx).Exec()
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (r *v3poolDBRepo) UpdatePoolsLowerUpperNearTicks(pools []models.UniswapV3Pool) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, pool := range pools {
		queryMap := map[string]any{
			models.UNISWAP_V3_POOL_TICK_LOWER: pool.TickLower,
			models.UNISWAP_V3_POOL_TICK_UPPER: pool.TickUpper,
			models.UNISWAP_V3_POOL_NEAR_TICKS: pool.NearTicksJSON,
		}

		query := psql.
			Update(
				models.UNISWAP_V3_POOL_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: pool.Address, models.UNISWAP_V3_POOL_CHAINID: pool.ChainID})

		_, err = query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
