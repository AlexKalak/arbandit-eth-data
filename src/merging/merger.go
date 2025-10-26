package merging

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/external/bybitapi"
	"github.com/alexkalak/go_market_analyze/src/external/rpcinteraction"
	"github.com/alexkalak/go_market_analyze/src/external/subgraphs"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/periphery/pgdatabase"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v2pairsrepo"
	"github.com/alexkalak/go_market_analyze/src/repo/exchangerepo/v3poolsrepo"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenlist"
	"github.com/alexkalak/go_market_analyze/src/repo/tokenrepo"
)

var USD_STABLECOIN_SYMBOLS = []string{
	"USDT",
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
const defaultToken0Holding = 0
const defaultToken1Holding = 0
const defaultIsDusty = true
const defaultBlockNumber = 0
const defaultSqrtPriceX96 = 0
const defaultLiquidity = 0
const defaultTick = 0
const defaultTickSpacing = 0
const defaultTickLower = 0
const defaultTickUpper = 0
const defaultNearTicks = "[]"

type Merger struct {
	BybitApi       *bybitapi.BybitApi
	database       *pgdatabase.PgDatabase
	TokenList      *tokenlist.TokenList
	SubgraphClient *subgraphs.SubgraphClient
	TokenRepo      tokenrepo.TokenRepo
	V3PoolsRepo    v3poolsrepo.V3PoolDBRepo
	V2PairsRepo    v2pairsrepo.V2PairRepo
	RpcClient      *rpcinteraction.RpcInteractionClient
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
var singleton *Merger

func New() (*Merger, error) {
	if singleton != nil {
		return singleton, nil
	}

	database, err := pgdatabase.New()
	if err != nil {
		return nil, err
	}
	fmt.Println("got database")
	tokenList, err := tokenlist.New()
	if err != nil {
		return nil, err
	}
	fmt.Println("got tokenlist")
	bybitApi, err := bybitapi.New()
	if err != nil {
		return nil, err
	}
	fmt.Println("got bybit")
	graphApi, err := subgraphs.New()
	if err != nil {
		return nil, err
	}
	fmt.Println("got graphapi")
	tokenRepo, err := tokenrepo.New()
	if err != nil {
		return nil, err
	}
	fmt.Println("got tokenrepo")
	v2pairsRepo, err := v2pairsrepo.New()
	if err != nil {
		return nil, err
	}
	v3poolsrepo, err := v3poolsrepo.NewDBRepo()
	if err != nil {
		return nil, err
	}

	fmt.Println("got exchangerepo")
	rpcClient, err := rpcinteraction.New()
	if err != nil {
		panic(err)
	}

	singleton = &Merger{
		BybitApi:       bybitApi,
		database:       database,
		TokenList:      tokenList,
		SubgraphClient: graphApi,
		TokenRepo:      tokenRepo,
		V2PairsRepo:    v2pairsRepo,
		V3PoolsRepo:    v3poolsrepo,
		RpcClient:      rpcClient,
	}

	return singleton, nil
}

func (m *Merger) MergeTokens() error {
	// respTokens, err := m.BybitApi.GetSpotInstruments()
	// if err != nil {
	// 	fmt.Println("error getting spot instruments: ", err)
	// 	return err
	// }

	db, err := m.database.GetDB()
	if err != nil {
		return err
	}

	addressesUsed := map[string]any{}
	tokenList, err := m.TokenList.GetAllTokens()
	if err != nil {
		return err
	}

	query := psql.Insert(models.TOKENS_TABLE).
		Columns(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
		)

	for _, tokenFromList := range tokenList {
		_, alreadyAdded := addressesUsed[tokenFromList.Address]
		if alreadyAdded {
			continue
		}

		token := models.Token{
			Name:     tokenFromList.Name,
			Symbol:   tokenFromList.Symbol,
			Address:  tokenFromList.Address,
			ChainID:  tokenFromList.ChainID,
			LogoURI:  tokenFromList.LogoURI,
			Decimals: tokenFromList.Decimals,
		}

		query = query.Values(token.Name, token.Symbol, token.Address, token.ChainID, token.LogoURI, token.Decimals)
		addressesUsed[token.Address] = new(any)
	}

	sqlText, args, err := query.ToSql()
	if err != nil {
		fmt.Println("err: ", err)
	}
	fmt.Println("sql Txt: ", sqlText)
	fmt.Println("args: ", args)

	res, err := query.RunWith(db).Exec()
	if err != nil {
		fmt.Println("Error running merging bybit tokens query", err)
		return err
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		fmt.Println("Error getting rows affected from merging bybit query", err)

	}
	fmt.Println("Rows affected while merging bybit tokens: ", rowsAffected)

	return nil
}
