package tokenrepo

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/common/helpers"
	"github.com/alexkalak/go_market_analyze/common/models"
	"github.com/alexkalak/go_market_analyze/common/periphery/pgdatabase"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type TokenRepo interface {
	GetTokens() ([]*models.Token, error)
	GetTokenByIdentificator(identificator models.TokenIdentificator) (*models.Token, error)
	GetTokensBySymbolsAndChainID(symbols []string, chainID uint) ([]*models.Token, error)
	GetTokensByAddressesAndChainID(symbols []string, chainID uint) ([]*models.Token, error)
	GetTokensByChainID(chainID uint) ([]*models.Token, error)
	UpdateTokens(tokens []*models.Token) error
	UpdateTokenPriceImpacts(impacts []*models.TokenPriceImpact) error
	UpdateTokenPriceImpactAndTokenPrice(token *models.Token, impact *models.TokenPriceImpact) error
}

type TokenDBRepoDependencies struct {
	Database *pgdatabase.PgDatabase
}

func (d *TokenDBRepoDependencies) validate() error {
	if d.Database == nil {
		return errors.New("token repo dependenices database cannot be nil")
	}

	return nil
}

type tokenRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func NewDBRepo(dependencies TokenDBRepoDependencies) (TokenRepo, error) {
	if err := dependencies.validate(); err != nil {
		return nil, err
	}

	return &tokenRepo{
		pgDatabase: dependencies.Database,
	}, nil
}

func (r *tokenRepo) GetTokens() ([]*models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
			models.TOKEN_USD_PRICE,
		).
		From(models.TOKENS_TABLE)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []*models.Token{}
	for rows.Next() {
		var token models.Token
		defiUSDPriceStr := ""
		err := rows.Scan(&token.Name, &token.Symbol, &token.Address, &token.ChainID, &token.LogoURI, &token.Decimals, &defiUSDPriceStr)
		if defiUSDPriceStr == "" {
			defiUSDPriceStr = "0"
		}

		defiUSDPrice := new(big.Float)

		_, ok := defiUSDPrice.SetString(defiUSDPriceStr)
		if !ok {
			fmt.Println("unable to parse: ", defiUSDPriceStr)
			return nil, errors.New("unable to parse defi price of token")
		}

		if err != nil {
			return nil, err
		}
		token.USDPrice = defiUSDPrice
		tokens = append(tokens, &token)
	}

	return tokens, nil
}

func (r *tokenRepo) GetTokensByChainID(chainID uint) ([]*models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := getTokensWithImpacts()

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	tokensMap := map[string]*models.Token{}
	var tokens = []*models.Token{}
	for rows.Next() {
		tokenWithImpacts := tokenWithImpactsRows{}

		err := tokenWithImpacts.scanFrom(rows)
		if err != nil {
			continue
		}

		tokenUSDPrice := new(big.Float)
		_, ok := tokenUSDPrice.SetString(tokenWithImpacts.tokenUSDPriceStr)
		if !ok {
			fmt.Println("unable to parse: ", tokenWithImpacts.tokenUSDPriceStr)
			return nil, errors.New("unable to parse defi price of token")
		}

		var token *models.Token
		if existingToken, ok := tokensMap[tokenWithImpacts.Address]; ok {
			token = existingToken
		} else {
			token = &models.Token{
				Name:     tokenWithImpacts.Name,
				Symbol:   tokenWithImpacts.Symbol,
				Address:  tokenWithImpacts.Address,
				ChainID:  tokenWithImpacts.ChainID,
				LogoURI:  tokenWithImpacts.LogoURI,
				Decimals: tokenWithImpacts.Decimals,
				USDPrice: tokenUSDPrice,
			}

			tokens = append(tokens, token)
			tokensMap[token.Address] = token
		}

		if tokenWithImpacts.isImpactValid() {
			impactPrice := new(big.Float)
			_, ok = impactPrice.SetString(tokenWithImpacts.ImpactPriceStr.String)
			if !ok {
				impactPrice = big.NewFloat(0)
			}

			impactImpact := new(big.Int)
			_, ok = impactImpact.SetString(tokenWithImpacts.ImpactImpactStr.String, 10)
			if !ok {
				impactImpact = big.NewInt(0)
			}

			tokenPriceImpact := models.TokenPriceImpact{
				ChainID:            uint(tokenWithImpacts.ImpactChainID.Int64),
				TokenAddress:       tokenWithImpacts.ImpactTokenAddress.String,
				ExchangeIdentifier: tokenWithImpacts.ImpactExchangeIdentifier.String,
				USDPrice:           impactPrice,
				Impact:             impactImpact,
			}

			impacts := token.GetImpacts()
			impacts = append(impacts, &tokenPriceImpact)
			token.SetImpacts(impacts)
			if len(impacts) > 1 {
				fmt.Println("Got existing token, appended impacts: ", helpers.GetJSONString(impacts))
			}

		}

	}

	return tokens, nil
}

func (r *tokenRepo) GetTokenByIdentificator(identificator models.TokenIdentificator) (*models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
			models.TOKEN_USD_PRICE,
		).
		From(models.TOKENS_TABLE).
		Where(sq.Eq{models.TOKEN_ADDRESS: identificator.Address, models.TOKEN_CHAINID: identificator.ChainID})

	token := models.Token{}
	defiUSDPriceStr := ""
	err = query.
		RunWith(db).
		QueryRow().
		Scan(&token.Name, &token.Symbol, &token.Address, &token.ChainID, &token.LogoURI, &token.Decimals, &defiUSDPriceStr)

	if err != nil {
		return nil, err
	}
	if defiUSDPriceStr == "" {
		defiUSDPriceStr = "0"
	}
	defiUSDPrice := new(big.Float)

	_, ok := defiUSDPrice.SetString(defiUSDPriceStr)
	if !ok {
		fmt.Println("unable to parse: ", defiUSDPriceStr)
		return nil, errors.New("unable to parse defi price of token")
	}

	token.USDPrice = defiUSDPrice

	return &token, nil
}

func (r *tokenRepo) GetTokensBySymbolsAndChainID(symbols []string, chainID uint) ([]*models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
			models.TOKEN_USD_PRICE,
		).
		From(models.TOKENS_TABLE).
		Where(sq.Eq{models.TOKEN_SYMBOL: symbols, models.TOKEN_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []*models.Token{}
	for rows.Next() {
		var token models.Token
		defiUSDPriceStr := ""
		err := rows.Scan(&token.Name, &token.Symbol, &token.Address, &token.ChainID, &token.LogoURI, &token.Decimals, &defiUSDPriceStr)
		if err != nil {
			return nil, err
		}

		defiUSDPrice := new(big.Float)
		_, ok := defiUSDPrice.SetString(defiUSDPriceStr)
		if !ok {
			return nil, errors.New("unable to parse defi price of token")
		}

		token.USDPrice = defiUSDPrice
		tokens = append(tokens, &token)
	}

	return tokens, nil
}

func (r *tokenRepo) GetTokensByAddressesAndChainID(addresses []string, chainID uint) ([]*models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return nil, err
	}

	query := psql.
		Select(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
			models.TOKEN_USD_PRICE,
		).
		From(models.TOKENS_TABLE).
		Where(sq.Eq{models.TOKEN_ADDRESS: addresses, models.TOKEN_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []*models.Token{}
	for rows.Next() {
		var token models.Token
		defiUSDPriceStr := ""
		err := rows.Scan(&token.Name, &token.Symbol, &token.Address, &token.ChainID, &token.LogoURI, &token.Decimals, &defiUSDPriceStr)
		if err != nil {
			return nil, err
		}

		defiUSDPrice := new(big.Float)
		_, ok := defiUSDPrice.SetString(defiUSDPriceStr)
		if !ok {
			return nil, errors.New("unable to parse defi price of token")
		}

		token.USDPrice = defiUSDPrice
		tokens = append(tokens, &token)
	}

	return tokens, nil
}

func (r *tokenRepo) UpdateTokens(tokens []*models.Token) error {
	if len(tokens) == 0 {
		return nil
	}

	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, token := range tokens {
		queryMap := map[string]any{
			models.TOKEN_NAME:      token.Name,
			models.TOKEN_SYMBOL:    token.Symbol,
			models.TOKEN_ADDRESS:   token.Address,
			models.TOKEN_CHAINID:   token.ChainID,
			models.TOKEN_LOGOURI:   token.LogoURI,
			models.TOKEN_DECIMALS:  token.Decimals,
			models.TOKEN_USD_PRICE: token.USDPrice.String(),
		}

		query := psql.
			Update(
				models.TOKENS_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{models.TOKEN_ADDRESS: token.Address, models.TOKEN_CHAINID: token.ChainID})

		_, err := query.RunWith(tx).Exec()
		if err != nil {
			return err
		}

		err = r.UpsertTokenPriceImpactsWithinTx(tx, token.GetImpacts())
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *tokenRepo) UpsertTokenPriceImpactsWithinTx(tx *sql.Tx, impacts []*models.TokenPriceImpact) error {
	if len(impacts) == 0 {
		return nil
	}

	for _, impact := range impacts {
		queryMap := map[string]any{
			models.TOKEN_PRICE_IMPACT_CHAIN_ID:            impact.ChainID,
			models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS:       impact.TokenAddress,
			models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER: impact.ExchangeIdentifier,
			models.TOKEN_PRICE_IMPACT_USD_PRICE:           impact.USDPrice.Text('f', -1),
			models.TOKEN_PRICE_IMPACT_IMPACT:              impact.Impact.String(),
		}
		setClauses := []string{}
		for col := range queryMap {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}

		query := psql.
			Insert(models.TOKEN_PRICE_IMPACT_TABLE).
			Columns(
				models.TOKEN_PRICE_IMPACT_CHAIN_ID,
				models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS,
				models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER,
				models.TOKEN_PRICE_IMPACT_USD_PRICE,
				models.TOKEN_PRICE_IMPACT_IMPACT,
			).
			Values(
				impact.ChainID,
				impact.TokenAddress,
				impact.ExchangeIdentifier,
				impact.USDPrice.Text('f', -1),
				impact.Impact.String(),
			).
			Suffix(fmt.Sprintf(`
				ON CONFLICT (%s, %s, %s)
				DO UPDATE SET %s`,
				models.TOKEN_PRICE_IMPACT_CHAIN_ID,
				models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS,
				models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER,
				strings.Join(setClauses, ", "),
			))

		_, err := query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *tokenRepo) UpdateTokenPriceImpacts(impacts []*models.TokenPriceImpact) error {
	if len(impacts) == 0 {
		return nil
	}

	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, impact := range impacts {
		queryMap := map[string]any{
			models.TOKEN_PRICE_IMPACT_CHAIN_ID:            impact.ChainID,
			models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS:       impact.TokenAddress,
			models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER: impact.ExchangeIdentifier,
			models.TOKEN_PRICE_IMPACT_USD_PRICE:           impact.USDPrice.Text('f', -1),
			models.TOKEN_PRICE_IMPACT_IMPACT:              impact.Impact.String(),
		}

		query := psql.
			Update(
				models.TOKEN_PRICE_IMPACT_TABLE,
			).SetMap(queryMap).
			Where(sq.Eq{
				models.TOKEN_PRICE_IMPACT_CHAIN_ID:            impact.ChainID,
				models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS:       impact.TokenAddress,
				models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER: impact.ExchangeIdentifier,
			})

		_, err := query.RunWith(tx).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (r *tokenRepo) UpdateTokenPriceImpactAndTokenPrice(token *models.Token, impact *models.TokenPriceImpact) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	queryMapForToken := map[string]any{
		models.TOKEN_USD_PRICE: token.USDPrice.Text('f', -1),
	}

	query1 := psql.
		Update(
			models.TOKENS_TABLE,
		).SetMap(queryMapForToken).
		Where(sq.Eq{models.TOKEN_ADDRESS: token.Address, models.TOKEN_CHAINID: token.ChainID})

	queryMapForImpact := map[string]any{
		models.TOKEN_PRICE_IMPACT_CHAIN_ID:            impact.ChainID,
		models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS:       impact.TokenAddress,
		models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER: impact.ExchangeIdentifier,
		models.TOKEN_PRICE_IMPACT_USD_PRICE:           impact.USDPrice.Text('f', -1),
		models.TOKEN_PRICE_IMPACT_IMPACT:              impact.Impact.String(),
	}

	query2 := psql.
		Update(
			models.TOKEN_PRICE_IMPACT_TABLE,
		).SetMap(queryMapForImpact).
		Where(sq.Eq{
			models.TOKEN_PRICE_IMPACT_CHAIN_ID:            impact.ChainID,
			models.TOKEN_PRICE_IMPACT_TOKEN_ADDRESS:       impact.TokenAddress,
			models.TOKEN_PRICE_IMPACT_EXCHANGE_IDENTIFIER: impact.ExchangeIdentifier,
		})

	_, err = query1.RunWith(tx).Exec()
	if err != nil {
		return err
	}
	_, err = query2.RunWith(tx).Exec()
	if err != nil {
		return err
	}

	return tx.Commit()

}
