package v3poolsrepo

import (
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/periphery/pgdatabase"
)

type V3PoolDBRepo interface {
	GetPoolByPoolIdentificator(poolIdentificator models.V3PoolIdentificator) (models.UniswapV3Pool, error)
	GetPools() ([]models.UniswapV3Pool, error)
	GetPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error)
	GetNotDustyPoolsByChainID(chainID uint) ([]models.UniswapV3Pool, error)
	UpdatePoolsLiquiditySqrtPriceTickAndTokenHoldingsBlockNumber(pools []models.UniswapV3Pool) error
	UpdatePoolsIsDusty(pools []models.UniswapV3Pool) error
	UpdatePoolsLowerUpperNearTicks(pools []models.UniswapV3Pool) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type v3poolDBRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func NewDBRepo() (V3PoolDBRepo, error) {
	pgDatabase, err := pgdatabase.New()
	if err != nil {
		return &v3poolDBRepo{}, err
	}

	return &v3poolDBRepo{
		pgDatabase: pgDatabase,
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

			models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
			models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
		).
		From(models.UNISWAP_V3_POOL_TABLE).
		Where(sq.Eq{models.UNISWAP_V3_POOL_ADDRESS: poolIdentificator.Address, models.UNISWAP_V3_POOL_CHAINID: poolIdentificator.ChainID})

	var pool models.UniswapV3Pool

	sqrtPriceX96Str := ""
	liquidityStr := ""

	token0HoldingStr := ""
	token1HoldingStr := ""
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
			&token0HoldingStr,
			&token1HoldingStr,
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

	token0Holding := new(big.Int)
	_, ok = token0Holding.SetString(token0HoldingStr, 10)
	if !ok {
		token0Holding = big.NewInt(0)
	}
	token1Holding := new(big.Int)
	_, ok = token1Holding.SetString(token1HoldingStr, 10)
	if !ok {
		token1Holding = big.NewInt(0)
	}

	pool.SqrtPriceX96 = sqrtPriceX96
	pool.Liquidity = liquidity
	pool.Token0Holding = token0Holding
	pool.Token1Holding = token1Holding

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

			models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
			models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
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

		token0HoldingStr := ""
		token1HoldingStr := ""

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
			&token0HoldingStr,
			&token1HoldingStr,
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

		token0Holding := new(big.Int)
		_, ok = token0Holding.SetString(token0HoldingStr, 10)
		if !ok {
			token0Holding = big.NewInt(0)
		}
		token1Holding := new(big.Int)
		_, ok = token1Holding.SetString(token1HoldingStr, 10)
		if !ok {
			token1Holding = big.NewInt(0)
		}

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity
		pool.Token0Holding = token0Holding
		pool.Token1Holding = token1Holding

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

			models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
			models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
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

		token0HoldingStr := ""
		token1HoldingStr := ""

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
			&token0HoldingStr,
			&token1HoldingStr,
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

		token0Holding := new(big.Int)
		_, ok = token0Holding.SetString(token0HoldingStr, 10)
		if !ok {
			token0Holding = big.NewInt(0)
		}
		token1Holding := new(big.Int)
		_, ok = token1Holding.SetString(token1HoldingStr, 10)
		if !ok {
			token1Holding = big.NewInt(0)
		}

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity
		pool.Token0Holding = token0Holding
		pool.Token1Holding = token1Holding

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

			models.UNISWAP_V3_POOL_TOKEN0_HOLDING,
			models.UNISWAP_V3_POOL_TOKEN1_HOLDING,
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

		token0HoldingStr := ""
		token1HoldingStr := ""

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
			&token0HoldingStr,
			&token1HoldingStr,
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

		token0Holding := new(big.Int)
		_, ok = token0Holding.SetString(token0HoldingStr, 10)
		if !ok {
			token0Holding = big.NewInt(0)
		}
		token1Holding := new(big.Int)
		_, ok = token1Holding.SetString(token1HoldingStr, 10)
		if !ok {
			token1Holding = big.NewInt(0)
		}

		pool.SqrtPriceX96 = sqrtPriceX96
		pool.Liquidity = liquidity
		pool.Token0Holding = token0Holding
		pool.Token1Holding = token1Holding

		pools = append(pools, pool)
	}

	return pools, nil
}

func (r *v3poolDBRepo) UpdatePoolsLiquiditySqrtPriceTickAndTokenHoldingsBlockNumber(pools []models.UniswapV3Pool) error {
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
			models.UNISWAP_V3_POOL_LIQUIDITY:      pool.Liquidity.String(),
			models.UNISWAP_V3_POOL_SQRTPRICEX96:   pool.SqrtPriceX96.String(),
			models.UNISWAP_V3_POOL_TICK:           pool.Tick,
			models.UNISWAP_V3_POOL_TICK_SPACING:   pool.TickSpacing,
			models.UNISWAP_V3_POOL_TOKEN0_HOLDING: pool.Token0Holding.String(),
			models.UNISWAP_V3_POOL_TOKEN1_HOLDING: pool.Token1Holding.String(),
			models.UNISWAP_V3_POOL_BLOCK_NUMBER:   pool.BlockNumber,
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
