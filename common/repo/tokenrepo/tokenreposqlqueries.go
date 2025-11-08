package tokenrepo

import (
	"database/sql"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/models"
)

type tokenWithImpactsRows struct {
	Name     string
	Symbol   string
	Address  string
	ChainID  uint
	LogoURI  string
	Decimals int

	tokenUSDPriceStr         string
	ImpactChainID            sql.NullInt64
	ImpactTokenAddress       sql.NullString
	ImpactExchangeIdentifier sql.NullString
	ImpactPriceStr           sql.NullString
	ImpactImpactStr          sql.NullString
}

func (q *tokenWithImpactsRows) scanFrom(rows *sql.Rows) error {
	return rows.Scan(
		&q.Name,
		&q.Symbol,
		&q.Address,
		&q.ChainID,
		&q.LogoURI,
		&q.Decimals,
		&q.tokenUSDPriceStr,
		&q.ImpactChainID,
		&q.ImpactTokenAddress,
		&q.ImpactExchangeIdentifier,
		&q.ImpactPriceStr,
		&q.ImpactImpactStr,
	)

}
func (q *tokenWithImpactsRows) isImpactValid() bool {
	return q.ImpactChainID.Valid &&
		q.ImpactTokenAddress.Valid &&
		q.ImpactTokenAddress.Valid &&
		q.ImpactExchangeIdentifier.Valid &&
		q.ImpactPriceStr.Valid &&
		q.ImpactImpactStr.Valid
}

func getTokensWithImpacts() *sq.SelectBuilder {
	query := psql.
		Select(
			"t."+models.TOKEN_NAME,
			"t."+models.TOKEN_SYMBOL,
			"t."+models.TOKEN_ADDRESS,
			"t."+models.TOKEN_CHAINID,
			"t."+models.TOKEN_LOGOURI,
			"t."+models.TOKEN_DECIMALS,
			"t."+models.TOKEN_USD_PRICE,

			"imp."+models.TOKEN_PRICE_IMPACT_CHAIN_ID,
			"imp."+models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS,
			"imp."+models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER,
			"imp."+models.TOKEN_PRICE_IMPACT_USD_PRICE,
			"imp."+models.TOKEN_PRICE_IMPACT_IMPACT,
		).
		From(models.TOKENS_TABLE + " t").
		LeftJoin(models.TOKEN_PRICE_IMPACT_TABLE + " imp ON imp.chain_id = t.chain_id AND imp.token_address = t.address")

	return &query
}
