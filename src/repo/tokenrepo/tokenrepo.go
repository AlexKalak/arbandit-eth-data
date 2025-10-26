package tokenrepo

import (
	"errors"
	"fmt"
	"math/big"

	sq "github.com/Masterminds/squirrel"
	"github.com/alexkalak/go_market_analyze/src/models"
	"github.com/alexkalak/go_market_analyze/src/periphery/pgdatabase"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

type TokenRepo interface {
	GetTokens() ([]models.Token, error)
	GetTokenByIdentificator(models.TokenIdentificator) (models.Token, error)
	GetTokensBySymbolsAndChainID(symbols []string, chainID uint) ([]models.Token, error)
	GetTokensByChainID(chainID uint) ([]models.Token, error)
	UpdateTokens(tokens []models.Token) error
}
type tokenRepo struct {
	pgDatabase *pgdatabase.PgDatabase
}

func New() (TokenRepo, error) {
	pgDatabase, err := pgdatabase.New()
	if err != nil {
		return &tokenRepo{}, err
	}

	return &tokenRepo{
		pgDatabase: pgDatabase,
	}, nil
}

func (r *tokenRepo) GetTokens() ([]models.Token, error) {
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
			models.TOKEN_DEFI_USD_PRICE,
		).
		From(models.TOKENS_TABLE)

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []models.Token{}
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
		token.DefiUSDPrice = defiUSDPrice
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (r *tokenRepo) GetTokensByChainID(chainID uint) ([]models.Token, error) {
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
			models.TOKEN_DEFI_USD_PRICE,
		).
		From(models.TOKENS_TABLE).
		Where(sq.Eq{models.TOKEN_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []models.Token{}
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
		token.DefiUSDPrice = defiUSDPrice
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (r *tokenRepo) GetTokenByIdentificator(identificator models.TokenIdentificator) (models.Token, error) {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return models.Token{}, err
	}

	query := psql.
		Select(
			models.TOKEN_NAME,
			models.TOKEN_SYMBOL,
			models.TOKEN_ADDRESS,
			models.TOKEN_CHAINID,
			models.TOKEN_LOGOURI,
			models.TOKEN_DECIMALS,
			models.TOKEN_DEFI_USD_PRICE,
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
		return models.Token{}, err
	}
	if defiUSDPriceStr == "" {
		defiUSDPriceStr = "0"
	}
	defiUSDPrice := new(big.Float)

	_, ok := defiUSDPrice.SetString(defiUSDPriceStr)
	if !ok {
		fmt.Println("unable to parse: ", defiUSDPriceStr)
		return models.Token{}, errors.New("unable to parse defi price of token")
	}

	token.DefiUSDPrice = defiUSDPrice

	return token, nil
}

func (r *tokenRepo) GetTokensBySymbolsAndChainID(symbols []string, chainID uint) ([]models.Token, error) {
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
			models.TOKEN_DEFI_USD_PRICE,
		).
		From(models.TOKENS_TABLE).
		Where(sq.Eq{models.TOKEN_SYMBOL: symbols, models.TOKEN_CHAINID: chainID})

	rows, err := query.
		RunWith(db).
		Query()
	if err != nil {
		return nil, err
	}

	var tokens = []models.Token{}
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

		token.DefiUSDPrice = defiUSDPrice
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (r *tokenRepo) UpdateTokens(tokens []models.Token) error {
	db, err := r.pgDatabase.GetDB()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, token := range tokens {
		queryMap := map[string]interface{}{
			models.TOKEN_NAME:           token.Name,
			models.TOKEN_SYMBOL:         token.Symbol,
			models.TOKEN_ADDRESS:        token.Address,
			models.TOKEN_CHAINID:        token.ChainID,
			models.TOKEN_LOGOURI:        token.LogoURI,
			models.TOKEN_DECIMALS:       token.Decimals,
			models.TOKEN_DEFI_USD_PRICE: token.DefiUSDPrice.String(),
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
	}
	return tx.Commit()
}
