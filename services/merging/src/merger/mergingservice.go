package merger

import (
	"context"
	"errors"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/external/rpcclient"
	"github.com/alexkalak/go_market_analyze/common/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/common/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/common/repo/tokenrepo"
)

var USD_STABLECOIN_ADDRESSES = []string{
	"0xdac17f958d2ee523a2206206994597c13d831ec7",
	// "USDC",
	// "DAI",
	// "TUSD",
	// "PAX",
	// "GUSD",
	// "HUSD",
	// "USDCV",
}

const defaultAmount0 = 0
const defaultAmount1 = 0
const defaultZfo10USDRate = 0
const defaultNonZfo10USDRate = 0
const defaultIsDusty = true
const defaultBlockNumber = 0
const defaultSqrtPriceX96 = 0
const defaultLiquidity = 0
const defaultTick = 0
const defaultTickSpacing = 0
const defaultTickLower = 0
const defaultTickUpper = 0
const defaultNearTicks = "[]"

type MergerDependencies struct {
	Database       *pgdatabase.PgDatabase
	SubgraphClient subgraphs.SubgraphClient
	V3PoolsDBRepo  v3poolsrepo.V3PoolDBRepo
	TokenRepo      tokenrepo.TokenRepo
	RpcClient      rpcclient.RpcClient
}

func (d *MergerDependencies) validate() error {
	if d.Database == nil {
		return errors.New("merger dependencies database cannot be nil")
	}
	if d.SubgraphClient == nil {
		return errors.New("merger dependencies subgraphclient cannot be nil")
	}
	if d.V3PoolsDBRepo == nil {
		return errors.New("merger dependencies v3pools db repo cannot be nil")
	}
	if d.TokenRepo == nil {
		return errors.New("merger dependencies token db repo cannot be nil")
	}
	if d.RpcClient == nil {
		return errors.New("merger dependencies rpc client cannot be nil")
	}

	return nil
}

type Merger interface {
	MergeTokens(chainID uint) error
	MergePools(chainID uint) error
	MergePoolsData(ctx context.Context, chainID uint, blockNumber *big.Int) error
	MergePoolsTicks(ctx context.Context, chainID uint, blockNumber *big.Int) error
	ValidateV3PoolsAndComputeAverageUSDPrice(chainID uint) error
}

type merger struct {
	database       *pgdatabase.PgDatabase
	subgraphClient subgraphs.SubgraphClient
	tokenRepo      tokenrepo.TokenRepo
	v3PoolsDBRepo  v3poolsrepo.V3PoolDBRepo
	rpcClient      rpcclient.RpcClient
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func NewMerger(dependencies MergerDependencies) (Merger, error) {
	err := dependencies.validate()
	if err != nil {
		return &merger{}, err
	}

	return &merger{
		database:       dependencies.Database,
		subgraphClient: dependencies.SubgraphClient,
		v3PoolsDBRepo:  dependencies.V3PoolsDBRepo,
		tokenRepo:      dependencies.TokenRepo,
		rpcClient:      dependencies.RpcClient,
	}, nil
}
